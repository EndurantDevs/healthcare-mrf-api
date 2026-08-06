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
import shutil
import stat
import struct
import subprocess
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import ijson
from sqlalchemy import cast, func, literal
from sqlalchemy.dialects.postgresql import JSONB

from api.ptg2_code_filters import INFERRED_PROVIDER_TAXONOMY_RULES
from db.connection import db
from db.models import (
    ImportLog,
    PTG2CurrentPlanSource,
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
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
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
from process.ptg_parts.artifact_streams import (
    load_json_artifact,
    logical_artifact_identity,
    open_json_artifact_stream,
    stream_logical_artifact,
)
from process.ptg_parts.allowed_amounts import (
    PTG2_ALLOWED_AMOUNT_CONTRACT,
    PTG2_ALLOWED_AMOUNT_TABLE_NAMES,
    _process_allowed_amounts_file,
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
from process.ptg_parts.ptg2_artifact_blobs import delete_ptg2_artifacts_for_snapshot
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    guard_attempt_rows,
    has_stale_metadata_marker,
    is_stale_metadata_fence_error,
    lock_writable_snapshot,
    raise_stale_metadata_fence,
)
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2_V4_STALE_METADATA_MARKER,
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
    PTG2_COPY_UPSERT_ROWS_ENV,
    PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS,
    PTG2_DEFAULT_RUST_EVENT_QUEUE,
    PTG2_DEFAULT_RUST_WORKERS,
    PTG2_DIRECT_COPY_SERVING_RATE_ENV,
    PTG2_DOWNLOAD_RETRIES_ENV,
    PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV,
    PTG2_DOWNLOAD_TASKS_ENV,
    PTG2_FAST_PROVIDER_UNION_ENV,
    PTG2_FILE_PROCESS_CONCURRENCY_ENV,
    PTG2_KEEP_PARTIAL_ENV,
    PTG2_MANIFEST_DIRECT_COPY_TASKS_ENV,
    PTG2_PROVIDER_BUCKET_COUNT_ENV,
    PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV,
    PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_TASKS_ENV,
    PTG2_RANGE_DOWNLOADS_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_WORKERS_ENV,
    PTG2_SOURCE_IMPORT_LOCK_ENABLED_ENV,
    PTG2_STREAMING_DEDUPE_ENV,
    TEST_TOC_FILES,
    TEST_TOC_JOBS,
    _env_bool,
    _env_int,
    _is_postgres_binary_v3_arch,
    _ptg2_snapshot_arch_from_env,
)
from process.ptg_parts.config import _should_auto_activate_ptg2_candidates
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    binding_option,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
    protected_frozen_tuple_presence,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding_transaction,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    assert_frozen_input_compatibility,
    bind_frozen_rate_set_to_scope,
    build_frozen_rate_jobs,
    frozen_rate_file_proof_sha256,
    normalize_frozen_rate_file_set,
    validate_frozen_processed_results,
)
from process.ptg_parts.copy_load import (
    _copy_ignore_ptg2_objects,
    _copy_insert_ptg2_objects,
    _copy_upsert_ptg2_objects,
)
from process.ptg_parts.db_tables import (
    _estimated_table_rows,
    _exact_table_rows,
    _quote_ident,
    _has_rows_in_table,
    _is_table_available,
)
from process.ptg_parts.domain import (
    PTG2_ARTIFACT_RAW,
    PTG2_CANDIDATE_ACTIVATION_CONTRACT,
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
from process.ptg_parts.live_progress import (
    current_live_progress_context,
    reset_live_progress_context,
    set_live_progress_context,
    write_live_progress,
)
from process.ptg_parts.input_artifact_retention import (
    artifact_lease_context,
    guard_artifact_lease,
    release_current_artifact_lease,
)
from process.ptg_parts.progress import (
    PTGFileProgressCoordinator,
    _artifact_progress_position,
    _format_duration,
    _maybe_log_artifact_progress,
    _scale_stage_progress_pct,
    _utcnow,
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
from process.ptg_parts.provider_references import (
    _load_provider_references_from_file,
    _process_provider_reference_file,
)
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    membership_index_fence_metadata,
)
from process.ptg_parts.ptg2_manifest_publish import (
    PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    _copy_price_atom_member_file,
    _copy_price_set_summary_file,
    _copy_price_atom_file,
    _create_serving_stage_table,
    _ptg2_manifest_stage_table_name,
    _ptg2_manifest_support_stage_table,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    combine_provider_identifier_quarantines,
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_GENERATION,
    is_shared_layout_build_abandoned,
    reserve_shared_layout,
)
from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
)
from process.ptg_parts.ptg2_shared_gc import (
    PTG2SharedLayoutAbandonmentDeferred,
    abandon_owned_v4_layout,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    logical_plan_fields_for_job,
    normalized_full_rebuild_scope_digest,
    normalized_physical_artifact_identity,
    is_same_downloaded_physical_input,
    shared_logical_artifact_metadata,
    shared_physical_artifact_identity,
    shared_physical_input_identity,
    shared_snapshot_source_assignments,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    delete_unpublished_snapshot_sources,
    publish_shared_v3_snapshot_sources,
    publish_strict_shared_v3_layout,
    validate_reused_snapshot_sources,
)
from process.ptg_parts.ptg2_tax_identity_source_binding import (
    bind_tax_source_sidecars,
    build_tax_source_bindings,
)
from process.ptg_parts.ptg2_tax_identity_source_validation import (
    validate_reused_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    reserve_v4_shared_layout,
)
from process.ptg_parts.ptg2_v4_graph_compiler import (
    _resolve_v4_graph_compiler_binary,
    v4_graph_encoding_policy,
)
from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
    inferred_provider_taxonomy_rule_set_digest,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    validate_source_witness_manifest,
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
    _V4_EMPTY_NPI_NORMALIZATION_CONTRACT,
    _V4_EMPTY_NPI_NORMALIZATION_HASH_DOMAIN,
    _aiter_compact_serving_records_rust,
    _iter_compact_serving_records_rust,
    _iter_top_level_object_bytes_rust,
    _ptg2_rust_scanner_binary,
    _verify_v4_tin_only_audit,
)
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.snapshot_cleanup import (
    _cleanup_old_ptg2_source_tables,
    _drop_ptg2_snapshot_table_names,
    _drop_ptg2_snapshot_tables_for_manifest,
    _missing_snapshot_serving_resources,
    _snapshot_manifest_table_names,
)
from process.ptg_parts.snapshot_tables import (
    _normalize_source_key,
    _ptg2_snapshot_index_name,
    _ptg2_snapshot_table_name,
    _ptg2_snapshot_table_token,
)
from process.ptg_parts.source_download import (
    PTG2_DEFAULT_MAX_BYTES,
    PTG2ArtifactStageCounts,
    PTG2ArtifactStageFreshnessError,
    PTG2ArtifactStageObserver,
    PTG2FreshArtifactStageTracker,
    _download_ptg_job_artifact,
    _download_ptg_job_artifact_sync,
    _download_raw_artifact_ranges,
    _emit_download_progress,
    _format_eta_seconds,
    _iter_downloaded_ptg_jobs,
    _progress_job_index,
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
from process.ptg_parts.source_jobs import (
    _dedupe_preserve,
    _dedupe_ptg_jobs,
    _dedupe_rows_by,
    _filter_jobs_by_url_contains,
    _filter_reporting_plans,
    _load_toc_urls_from_file,
    _is_toc_body_file_location,
    _merge_ptg_job,
    _normalize_filter_values,
    _normalize_plan_payload,
    _plan_identity,
    _plan_matches_filters,
    _ptg_job_identity,
    parse_toc_catalog_entries,
)
from process.ptg_parts.source_pointers import (
    _current_source_snapshot_id,
    _acquire_source_pointer_gc_lock,
    _allowed_source_pointer_key,
    _activate_ptg2_source_candidate_in_transaction,
    _compare_and_swap_source_pointer,
    _stage_ptg2_source_candidate,
    activated_snapshot_attributes,
    _ptg2_plan_source_key,
    _publish_ptg2_source_pointers,
    _source_plan_rows,
)
from process.ptg_parts.source_versions import _record_source_version
from process.ptg_parts.table_setup import (
    PTG2_MODEL_CLASSES,
    PTG_CONTROL_TABLE_CLASS_NAMES,
    PTG_PROVIDER_REFERENCE_TABLE_CLASS_NAMES,
    _drop_ptg2_columns,
    _ensure_indexes,
    _ensure_ptg_dynamic_tables,
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
from process.url_security import fetch_max_bytes

logger = logging.getLogger(__name__)
_ptg2_monotonic = time.monotonic
_PTG2_PUBLISH_PROGRESS_INTERVAL_SECONDS = 4.0

PTG2_SOURCE_SCOPED_TEST_ENV = "HLTHPRT_PTG2_SOURCE_SCOPED_TEST"
PTG2_AUTO_ADDRESS_REFRESH_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH"
PTG2_AUTO_ADDRESS_REFRESH_TEST_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_TEST"
PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV = (
    "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_LIMIT_PER_SOURCE"
)
PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_PUBLISH"


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


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
    params_by_name: dict[str, Any] = {
        "refresh_mode": "full",
        "trigger_source_key": source_key,
        "trigger_snapshot_id": snapshot_id,
        "publish": _env_bool(PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV, True),
    }
    if test_mode:
        params_by_name["test_mode"] = True
    limit_per_source = max(_env_int(PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV, 0), 0)
    if limit_per_source:
        params_by_name["limit_per_source"] = limit_per_source
    return {
        "run_id": None,
        "importer": "entity-address-unified",
        "params": params_by_name,
        "idempotency_key": f"entity-address-unified:{source_key}:{snapshot_id}",
        "triggered_by": "ptg_import",
        "schedule_id": None,
        "subscription_id": None,
        "import_id": f"entity-address-unified:{import_run_id}",
    }


async def _enqueue_address_refresh_after_import(
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
    refresh_request = _ptg2_auto_address_refresh_payload(
        source_key=source_key,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        test_mode=test_mode,
    )
    try:
        from api.control_imports import create_import_run, ensure_import_run_table

        await ensure_import_run_table()
        run, created = await create_import_run(refresh_request)
        return {
            "status": "queued" if created else "existing",
            "created": bool(created),
            "run_id": run.get("run_id"),
            "importer": run.get("importer") or refresh_request["importer"],
            "idempotency_key": refresh_request["idempotency_key"],
            "params": refresh_request["params"],
        }
    except Exception as exc:
        logger.exception(
            "Failed to enqueue pricing address refresh after PTG import %s",
            import_run_id,
        )
        return {
            "status": "enqueue_failed",
            "error": str(exc),
            "idempotency_key": refresh_request["idempotency_key"],
            "params": refresh_request["params"],
        }


class PTG2SnapshotInProgressConflict(RuntimeError):
    """Raised when another delivery owns a deterministic snapshot build."""


_SAFE_FULL_REBUILD_METRIC_KEYS = frozenset(
    {
        "full_rebuild",
        "artifacts_observed",
        "raw_artifacts_total",
        "raw_artifacts_reused",
        "raw_artifacts_unique",
        "raw_artifacts_duplicate_identities",
        "logical_artifacts_total",
        "logical_artifacts_reused",
        "logical_artifacts_unique",
        "logical_artifacts_duplicate_identities",
        "logical_artifacts_deferred_hashes",
        "shared_layout_reused",
        "shared_layout_reused_at_seal",
        "existing_snapshot_reused",
        "finalizer_block_source_copy_bytes",
        "finalizer_block_staged_copy_bytes",
        "finalizer_block_source_payload_bytes",
        "finalizer_block_staged_payload_bytes",
        "finalizer_block_reused_payload_bytes",
        "finalizer_block_durable_reused_payload_bytes",
        "finalizer_block_same_copy_reused_payload_bytes",
        "finalizer_block_row_count",
        "finalizer_block_staged_payload_row_count",
        "finalizer_block_reused_payload_row_count",
        "finalizer_block_durable_reused_row_count",
        "finalizer_block_same_copy_reused_row_count",
        "finalizer_block_unique_block_count",
        "finalizer_block_existing_block_count",
        "finalizer_block_new_block_count",
        "finalizer_block_duplicate_block_row_count",
    }
)
_BOOLEAN_FULL_REBUILD_METRIC_KEYS = frozenset(
    {
        "full_rebuild",
        "shared_layout_reused",
        "shared_layout_reused_at_seal",
        "existing_snapshot_reused",
    }
)
_COUNT_FULL_REBUILD_METRIC_KEYS = (
    _SAFE_FULL_REBUILD_METRIC_KEYS - _BOOLEAN_FULL_REBUILD_METRIC_KEYS
)


class PTG2FullRebuildFreshnessError(RuntimeError):
    """Raised when a controlled rebuild encounters previously completed work."""

    def __init__(self, message: str, metrics_by_name: Mapping[str, Any]):
        super().__init__(message)
        self.metrics_by_name = _safe_full_rebuild_metrics(metrics_by_name)


def _safe_full_rebuild_metrics(
    metrics_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Allowlist and type-check proof before it crosses the PTG boundary."""

    safe_metrics_by_name: dict[str, Any] = {}
    for metric_name, metric_value in metrics_by_name.items():
        if (
            metric_name in _BOOLEAN_FULL_REBUILD_METRIC_KEYS
            and type(metric_value) is bool
        ):
            safe_metrics_by_name[metric_name] = metric_value
        elif (
            metric_name in _COUNT_FULL_REBUILD_METRIC_KEYS
            and type(metric_value) is int
            and metric_value >= 0
        ):
            safe_metrics_by_name[metric_name] = metric_value
    return safe_metrics_by_name


def _attach_full_rebuild_failure_metrics(
    error: BaseException,
    metrics_by_name: Mapping[str, Any],
) -> None:
    """Attach safe runtime proof to an exception crossing the control boundary."""

    safe_metrics_by_name = _safe_full_rebuild_metrics(metrics_by_name)
    if safe_metrics_by_name:
        setattr(
            error,
            "ptg_full_rebuild_metrics_by_name",
            safe_metrics_by_name,
        )


def full_rebuild_failure_metrics(error: BaseException) -> dict[str, Any]:
    """Read safe runtime proof from a failed or canceled controlled rebuild."""

    metrics_by_name = getattr(
        error,
        "ptg_full_rebuild_metrics_by_name",
        {},
    )
    if not isinstance(metrics_by_name, Mapping):
        return {}
    return _safe_full_rebuild_metrics(metrics_by_name)


def _ptg2_snapshot_conflict_update_values(
    statement: Any,
    table: Any,
    *,
    incoming_status: str | None,
) -> dict[str, Any]:
    """Build snapshot upsert values while preserving a failed candidate manifest."""

    update_values_by_column = {
        column.name: getattr(statement.excluded, column.name)
        for column in table.c
        if column.name != "snapshot_id"
    }
    if incoming_status != PTG2_STATUS_FAILED:
        return update_values_by_column
    empty_jsonb = cast(literal("{}"), JSONB)
    existing_manifest = func.coalesce(cast(table.c.manifest, JSONB), empty_jsonb)
    failure_manifest = func.coalesce(
        cast(statement.excluded.manifest, JSONB),
        empty_jsonb,
    )
    update_values_by_column["manifest"] = cast(
        existing_manifest.op("||")(failure_manifest),
        table.c.manifest.type,
    )
    return update_values_by_column


def _has_stale_metadata_marker(json_column: Any) -> Any:
    """Build the SQL predicate protecting a reconciled metadata row."""

    empty_jsonb = cast(literal("{}"), JSONB)
    envelope = func.coalesce(cast(json_column, JSONB), empty_jsonb)
    return envelope.op("?")(PTG2_V4_STALE_METADATA_MARKER)


async def _store_fenced_snapshot_state(
    session: Any,
    statement: Any,
    table: Any,
    snapshot_attributes: dict[str, Any],
    *,
    is_snapshot_claim: bool,
) -> dict[str, Any]:
    """Store one snapshot state while its exact attempt remains writable."""

    schema_name = resolve_ptg2_schema()
    await guard_attempt_rows(
        session,
        db,
        schema_name=schema_name,
        table_name=PTG2Snapshot.__tablename__,
        attempt_rows=[snapshot_attributes],
    )
    stored_row = await statement.first()
    await guard_attempt_rows(
        session,
        db,
        schema_name=schema_name,
        table_name=PTG2Snapshot.__tablename__,
        attempt_rows=[snapshot_attributes],
    )
    has_snapshot_claim = stored_row is not None
    if stored_row is None:
        stored_row = await (
            db.select(*table.c)
            .where(table.c.snapshot_id == snapshot_attributes["snapshot_id"])
            .first()
        )
    snapshot_state = _row_mapping(stored_row)
    if is_snapshot_claim:
        snapshot_state["snapshot_claim_status"] = (
            "acquired" if has_snapshot_claim else "existing"
        )
    return snapshot_state


async def _push_ptg2_snapshot_preserving_publication(
    snapshot_attributes: dict[str, Any],
    *,
    initial_import_run_by_field: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = PTG2Snapshot.__table__
    statement = db.insert(table).values(snapshot_attributes)
    update_values_by_column = _ptg2_snapshot_conflict_update_values(
        statement,
        table,
        incoming_status=snapshot_attributes.get("status"),
    )
    is_snapshot_claim = snapshot_attributes.get("status") == PTG2_STATUS_BUILDING
    conflict_where = (
        table.c.status == PTG2_STATUS_FAILED
        if is_snapshot_claim
        else table.c.status.is_distinct_from(PTG2_STATUS_PUBLISHED)
    )
    conflict_where = conflict_where & ~_has_stale_metadata_marker(table.c.manifest)
    statement = statement.on_conflict_do_update(
        index_elements=["snapshot_id"],
        set_=update_values_by_column,
        where=conflict_where,
    ).returning(*table.c)

    async with db.transaction() as session:
        if is_snapshot_claim:
            await _acquire_source_pointer_gc_lock(session)
        snapshot_state = await _store_fenced_snapshot_state(
            session,
            statement,
            table,
            snapshot_attributes,
            is_snapshot_claim=is_snapshot_claim,
        )
        should_initialize_attempt = initial_import_run_by_field is not None and (
            snapshot_state.get("snapshot_claim_status") == "acquired"
            or _is_exact_building_attempt_retry(
                snapshot_state,
                initial_import_run_by_field,
            )
        )
        if should_initialize_attempt:
            await _push_fenced_import_run(initial_import_run_by_field)
        return snapshot_state


def _is_exact_building_attempt_retry(
    snapshot_state: Mapping[str, Any],
    import_run_attributes: Mapping[str, Any],
) -> bool:
    """Recognize the deterministic attempt while its source lock is held."""

    return (
        snapshot_state.get("snapshot_claim_status") == "existing"
        and snapshot_state.get("status") == PTG2_STATUS_BUILDING
        and snapshot_state.get("import_run_id")
        == import_run_attributes.get("import_run_id")
    )


async def _push_fenced_import_run(
    import_run_attributes: dict[str, Any],
) -> dict[str, Any]:
    """Upsert one run unless metadata reconciliation fenced the attempt."""

    table = PTG2ImportRun.__table__
    statement = db.insert(table).values(import_run_attributes)
    update_values_by_column = {
        column.name: getattr(statement.excluded, column.name)
        for column in table.c
        if column.name != "import_run_id"
    }
    statement = statement.on_conflict_do_update(
        index_elements=["import_run_id"],
        set_=update_values_by_column,
        where=~_has_stale_metadata_marker(table.c.report),
    ).returning(*table.c)
    async with db.transaction() as session:
        await guard_attempt_rows(
            session,
            db,
            schema_name=resolve_ptg2_schema(),
            table_name=PTG2ImportRun.__tablename__,
            attempt_rows=[import_run_attributes],
        )
        stored_row = await statement.first()
        if stored_row is not None:
            return _row_mapping(stored_row)
        existing_row = await (
            db.select(*table.c)
            .where(table.c.import_run_id == import_run_attributes["import_run_id"])
            .first()
        )
        existing_state = _row_mapping(existing_row)
        if has_stale_metadata_marker(existing_state.get("report")):
            raise StaleMetadataFenceError("PTG import run was metadata-reconciled")
        return existing_state


async def _push_fenced_ptg2_plan_months(
    plan_month_entries: list[dict[str, Any]],
) -> None:
    """Upsert plan-month rows while holding every snapshot fence row."""

    table = PTG2PlanMonth.__table__
    statement = db.insert(table).values(plan_month_entries)
    update_values_by_column = {
        column.name: getattr(statement.excluded, column.name)
        for column in table.c
        if column.name not in set(PTG2PlanMonth.__my_index_elements__)
    }
    statement = statement.on_conflict_do_update(
        index_elements=list(PTG2PlanMonth.__my_index_elements__),
        set_=update_values_by_column,
    )
    schema_name = resolve_ptg2_schema()
    snapshot_ids = sorted(
        {
            str(entry.get("snapshot_id") or "")
            for entry in plan_month_entries
            if entry.get("snapshot_id")
        }
    )
    async with db.transaction() as session:
        for snapshot_id in snapshot_ids:
            await lock_writable_snapshot(
                session,
                db,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
            )
        await statement.status()


def _ptg2_model_schema_name(cls: Any) -> str:
    return (
        getattr(getattr(cls, "__table__", None), "schema", None)
        or resolve_ptg2_schema()
    )


async def _push_fenced_ptg2_objects_direct(
    object_entries: list[dict[str, Any]],
    cls: Any,
    *,
    rewrite: bool,
) -> None:
    """Use the ordinary writer only while the exact attempt is writable."""

    async with db.transaction() as session:
        await guard_attempt_rows(
            session,
            db,
            schema_name=_ptg2_model_schema_name(cls),
            table_name=cls.__tablename__,
            attempt_rows=object_entries,
        )
        try:
            await push_objects(
                object_entries,
                cls,
                rewrite=rewrite,
                use_copy=False,
            )
        except TypeError as exc:
            if "use_copy" not in str(exc):
                raise
            await push_objects(object_entries, cls, rewrite=rewrite)


async def _has_completed_specialized_ptg2_write(
    object_entries: list[dict[str, Any]],
    cls: Any,
    *,
    rewrite: bool,
) -> bool:
    """Try enabled PTG COPY paths before the ordinary fenced writer."""

    if cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        try:
            await _copy_ignore_ptg2_objects(object_entries, cls)
            return True
        except Exception as exc:
            if is_stale_metadata_fence_error(exc):
                raise_stale_metadata_fence(exc)
            logger.warning(
                "PTG2 copy/ignore fallback for %s: %s", cls.__tablename__, exc
            )
    if cls is PTG2ServingRate and _env_bool(
        PTG2_DIRECT_COPY_SERVING_RATE_ENV,
        False,
    ):
        try:
            await _copy_insert_ptg2_objects(object_entries, cls)
            return True
        except Exception as exc:
            if is_stale_metadata_fence_error(exc):
                raise_stale_metadata_fence(exc)
            logger.warning(
                "PTG2 direct COPY fallback for %s: %s", cls.__tablename__, exc
            )
    if rewrite and len(object_entries) >= max(
        _env_int(PTG2_COPY_UPSERT_ROWS_ENV, 250),
        1,
    ):
        try:
            await _copy_upsert_ptg2_objects(object_entries, cls)
            return True
        except Exception as exc:
            if is_stale_metadata_fence_error(exc):
                raise_stale_metadata_fence(exc)
            logger.warning(
                "PTG2 copy/upsert fallback for %s: %s", cls.__tablename__, exc
            )
    return False


async def _push_ptg2_objects(
    object_entries: list[dict[str, Any]],
    cls,
    rewrite: bool = True,
    initial_import_run_by_field: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Route PTG writes through their lifecycle-fenced storage path."""

    if object_entries and cls is PTG2Snapshot and rewrite:
        if len(object_entries) != 1:
            raise ValueError("PTG snapshot state writes must contain exactly one row")
        initial_run_kwargs = (
            {"initial_import_run_by_field": (initial_import_run_by_field)}
            if initial_import_run_by_field is not None
            else {}
        )
        return await _push_ptg2_snapshot_preserving_publication(
            object_entries[0],
            **initial_run_kwargs,
        )
    if object_entries and cls is PTG2ImportRun and rewrite:
        if len(object_entries) != 1:
            raise ValueError("PTG import-run writes must contain exactly one row")
        return await _push_fenced_import_run(object_entries[0])
    if object_entries and cls is PTG2PlanMonth and rewrite:
        await _push_fenced_ptg2_plan_months(object_entries)
        return None
    if object_entries and await _has_completed_specialized_ptg2_write(
        object_entries,
        cls,
        rewrite=rewrite,
    ):
        return None
    await _push_fenced_ptg2_objects_direct(object_entries, cls, rewrite=rewrite)


def _ptg2_copy_file_row_count(path: Path) -> int:
    if not path.exists() or path.stat().st_size <= 0:
        return 0
    with path.open("rb") as fp:
        return sum(1 for _line in fp)


def _collect_ptg2_manifest_sidecar_artifacts(
    sidecar_paths: dict[str, Path | None],
    *,
    provider_group_tax_identity_artifact: Mapping[str, Any] | None = None,
    membership_graph_metrics: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Collect authenticated graph sidecars without retaining scratch state."""

    artifacts_by_kind: dict[str, dict[str, Any]] = {}
    for artifact_kind, artifact_path in sidecar_paths.items():
        if artifact_kind == "provider_npi_scope":
            continue
        if artifact_kind == "provider_group_tax_identity":
            if artifact_path is None and provider_group_tax_identity_artifact is None:
                continue
            if (
                artifact_path is None
                or not artifact_path.exists()
                or artifact_path.stat().st_size <= 0
            ):
                raise RuntimeError(
                    "PTG V4 provider-group tax identity artifact is missing"
                )
            artifacts_by_kind[artifact_kind] = (
                _validated_provider_group_tax_identity_artifact(
                    artifact_path,
                    provider_group_tax_identity_artifact,
                )
            )
            continue
        artifact_by_field = _membership_sidecar_artifact(
            artifact_kind,
            artifact_path,
        )
        if artifact_by_field is not None:
            artifacts_by_kind[artifact_kind] = artifact_by_field
    scope_path = sidecar_paths.get("provider_npi_scope")
    if scope_path is not None:
        npi_group = artifacts_by_kind.get("provider_npi_group")
        summary_by_field = dict(membership_graph_metrics or {})
        if npi_group is None:
            raise RuntimeError(
                "PTG V4 provider NPI scope lacks its reciprocal membership"
            )
        artifacts_by_kind["provider_npi_scope"] = (
            _validated_provider_npi_scope_artifact(
                scope_path,
                summary=summary_by_field,
                provider_npi_group=npi_group,
            )
        )
    return artifacts_by_kind


def _membership_sidecar_artifact(
    artifact_kind: str,
    artifact_path: Path | None,
) -> dict[str, Any] | None:
    """Build one present nonempty membership-sidecar manifest entry."""

    if (
        artifact_path is None
        or not artifact_path.exists()
        or artifact_path.stat().st_size <= 0
    ):
        return None
    digest, byte_count = sha256_file(artifact_path)
    record_format = PTG2_MANIFEST_MEMBERSHIP_FORMAT
    with artifact_path.open("rb") as artifact_file:
        if artifact_file.read(8) == b"PTG2MNDS":
            record_format = PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT
    return {
        "name": artifact_kind,
        "path": str(artifact_path),
        "record_format": record_format,
        "sha256": digest,
        "byte_count": byte_count,
        **membership_index_fence_metadata(artifact_path),
    }


_PTG2_PROVIDER_NPI_SCOPE_FORMAT = "ptg2_provider_npi_scope_pg_binary_int8_v1"
_PTG2_PROVIDER_NPI_SCOPE_BINDING_CONTRACT = (
    "provider_npi_scope_to_provider_npi_group_v1"
)
_PTG2_PROVIDER_NPI_SCOPE_BINDING_DOMAIN = b"ptg2:v4:provider-npi-scope-binding:v1\x00"
_PTG2_PROVIDER_NPI_SCOPE_SHARD_BINDING_CONTRACT = "provider_npi_scope_shard_binding_v1"
_PTG2_PROVIDER_NPI_SCOPE_SHARD_BINDING_DOMAIN = (
    b"ptg2:v4:provider-npi-scope-shard-binding:v1\x00"
)
_PTG2_PROVIDER_NPI_SCOPE_RETENTION_CONTRACT = "shared_v4_publication_scratch_v1"
_PTG2_PG_BINARY_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)


def _validated_provider_npi_scope_artifact(
    path: Path,
    *,
    summary: Mapping[str, Any],
    provider_npi_group: Mapping[str, Any],
) -> dict[str, Any]:
    """Authenticate one source-local NPI scope against its reverse graph."""

    path_metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(path_metadata.st_mode):
        raise RuntimeError("PTG V4 provider NPI scope is not a regular file")
    expected_path = str(path)
    try:
        row_count = int(summary["provider_npi_scope_copy_rows"])
        reported_bytes = int(summary["provider_npi_scope_copy_bytes"])
        reciprocal_bytes = int(summary["provider_npi_group_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("PTG V4 provider NPI scope summary is invalid") from exc
    reciprocal_owner_count = int(provider_npi_group["owner_count"])
    reciprocal_member_count = int(provider_npi_group["member_count"])
    actual_bytes = path_metadata.st_size
    expected_bytes = len(_PTG2_PG_BINARY_COPY_HEADER) + row_count * 14 + 2
    if (
        summary.get("provider_npi_scope_copy_path") != expected_path
        or summary.get("provider_npi_scope_copy_format")
        != _PTG2_PROVIDER_NPI_SCOPE_FORMAT
        or row_count < 0
        or row_count != reciprocal_owner_count
        or reciprocal_bytes != int(provider_npi_group["byte_count"])
        or reported_bytes != actual_bytes
        or actual_bytes != expected_bytes
    ):
        raise RuntimeError(
            "PTG V4 provider NPI scope is inconsistent with its reverse graph"
        )
    _validate_provider_npi_scope_copy(path, row_count=row_count)
    digest, byte_count = sha256_file(path)
    binding_by_field = _provider_npi_scope_binding(
        digest=digest,
        byte_count=byte_count,
        row_count=row_count,
        provider_npi_group=provider_npi_group,
    )
    return {
        "name": "provider_npi_scope",
        "path": str(path),
        **binding_by_field,
        "binding_contract": _PTG2_PROVIDER_NPI_SCOPE_BINDING_CONTRACT,
        "binding_sha256": _provider_npi_scope_binding_sha256(binding_by_field),
        "retention_contract": _PTG2_PROVIDER_NPI_SCOPE_RETENTION_CONTRACT,
    }


def _validate_provider_npi_scope_copy(path: Path, *, row_count: int) -> None:
    """Validate the dense ordered PostgreSQL COPY payload."""

    previous_npi = 0
    with path.open("rb") as scope_file:
        if scope_file.read(len(_PTG2_PG_BINARY_COPY_HEADER)) != (
            _PTG2_PG_BINARY_COPY_HEADER
        ):
            raise RuntimeError("PTG V4 provider NPI scope COPY header is invalid")
        for _row_index in range(row_count):
            if (
                scope_file.read(2) != b"\x00\x01"
                or scope_file.read(4) != b"\x00\x00\x00\x08"
            ):
                raise RuntimeError("PTG V4 provider NPI scope COPY row is invalid")
            raw_npi = scope_file.read(8)
            if len(raw_npi) != 8:
                raise RuntimeError("PTG V4 provider NPI scope COPY row is truncated")
            npi = int.from_bytes(raw_npi, "big", signed=True)
            if npi <= previous_npi or not 1_000_000_000 <= npi <= 9_999_999_999:
                raise RuntimeError("PTG V4 provider NPI scope is not strict NPI order")
            previous_npi = npi
        if scope_file.read(2) != b"\xff\xff" or scope_file.read(1):
            raise RuntimeError("PTG V4 provider NPI scope COPY trailer is invalid")


def _provider_npi_scope_binding(
    *,
    digest: str,
    byte_count: int,
    row_count: int,
    provider_npi_group: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the fields that bind the NPI scope to its reciprocal graph."""

    return {
        "record_format": _PTG2_PROVIDER_NPI_SCOPE_FORMAT,
        "sha256": digest,
        "byte_count": byte_count,
        "row_count": row_count,
        "provider_npi_group_sha256": str(provider_npi_group["sha256"]),
        "provider_npi_group_record_format": str(provider_npi_group["record_format"]),
        "provider_npi_group_byte_count": int(provider_npi_group["byte_count"]),
        "provider_npi_group_owner_count": int(provider_npi_group["owner_count"]),
        "provider_npi_group_member_count": int(provider_npi_group["member_count"]),
        "provider_npi_group_member_global_count": int(
            provider_npi_group["member_global_count"]
        ),
    }


def _provider_npi_scope_binding_sha256(
    binding_by_field: Mapping[str, Any],
) -> str:
    """Hash the complete source-scope-to-reciprocal binding."""

    binding_digest_builder = hashlib.sha256()
    binding_digest_builder.update(_PTG2_PROVIDER_NPI_SCOPE_BINDING_DOMAIN)
    format_bytes = _PTG2_PROVIDER_NPI_SCOPE_FORMAT.encode("ascii")
    binding_digest_builder.update(len(format_bytes).to_bytes(4, "big"))
    binding_digest_builder.update(format_bytes)
    binding_digest_builder.update(bytes.fromhex(str(binding_by_field["sha256"])))
    binding_digest_builder.update(
        int(binding_by_field["byte_count"]).to_bytes(8, "big")
    )
    binding_digest_builder.update(int(binding_by_field["row_count"]).to_bytes(8, "big"))
    binding_digest_builder.update(
        bytes.fromhex(str(binding_by_field["provider_npi_group_sha256"]))
    )
    reciprocal_format = str(
        binding_by_field["provider_npi_group_record_format"]
    ).encode("ascii")
    binding_digest_builder.update(len(reciprocal_format).to_bytes(4, "big"))
    binding_digest_builder.update(reciprocal_format)
    binding_digest_builder.update(
        int(binding_by_field["provider_npi_group_byte_count"]).to_bytes(8, "big")
    )
    binding_digest_builder.update(
        int(binding_by_field["provider_npi_group_owner_count"]).to_bytes(8, "big")
    )
    binding_digest_builder.update(
        int(binding_by_field["provider_npi_group_member_count"]).to_bytes(8, "big")
    )
    binding_digest_builder.update(
        int(binding_by_field["provider_npi_group_member_global_count"]).to_bytes(
            8, "big"
        )
    )
    return binding_digest_builder.hexdigest()


def _bind_npi_scope_to_source_shard(
    sidecars: list[dict[str, Any]],
    *,
    source_shard_id: str,
) -> None:
    """Seal exact source provenance onto one temporary V4 scope artifact."""

    scope_rows = [
        sidecar for sidecar in sidecars if sidecar.get("name") == "provider_npi_scope"
    ]
    reciprocal_rows = [
        sidecar for sidecar in sidecars if sidecar.get("name") == "provider_npi_group"
    ]
    if not scope_rows:
        return
    if len(scope_rows) != 1 or len(reciprocal_rows) != 1:
        raise RuntimeError("PTG V4 provider NPI scope shard binding is incomplete")
    scope = scope_rows[0]
    reciprocal = reciprocal_rows[0]
    if any(
        scope.get(scope_name) != reciprocal.get(reciprocal_name)
        for scope_name, reciprocal_name in (
            ("provider_npi_group_sha256", "sha256"),
            ("provider_npi_group_record_format", "record_format"),
            ("provider_npi_group_byte_count", "byte_count"),
            ("provider_npi_group_owner_count", "owner_count"),
            ("provider_npi_group_member_count", "member_count"),
            (
                "provider_npi_group_member_global_count",
                "member_global_count",
            ),
        )
    ):
        raise RuntimeError("PTG V4 provider NPI scope reciprocal binding changed")
    digest = hashlib.sha256()
    digest.update(_PTG2_PROVIDER_NPI_SCOPE_SHARD_BINDING_DOMAIN)
    digest.update(bytes.fromhex(str(scope["binding_sha256"])))
    shard_bytes = source_shard_id.encode("utf-8")
    digest.update(len(shard_bytes).to_bytes(4, "big"))
    digest.update(shard_bytes)
    scope["shard_binding_contract"] = _PTG2_PROVIDER_NPI_SCOPE_SHARD_BINDING_CONTRACT
    scope["shard_binding_sha256"] = digest.hexdigest()


_PTG2_TAX_IDENTITY_MAGIC = b"PTG2TAX1"
_PTG2_TAX_IDENTITY_VERSION = 1
_PTG2_TAX_IDENTITY_RECORD_BYTES = 65
_PTG2_TAX_IDENTITY_POLICY_ID_RE = re.compile(
    r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})\Z"
)
_PTG2_TAX_IDENTITY_FRAME_FIELDS = frozenset(
    {
        "path",
        "bytes",
        "row_count",
        "provider_group_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
        "format",
        "version",
        "record_bytes",
        "token_policy_id",
        "normalization_contract",
        "hmac_contract",
        "sha256",
        "final",
    }
)


def _validate_tax_identity_summary_frame(
    artifact_path: Path,
    scanner_artifact_by_field: Mapping[str, Any] | None,
) -> tuple[str, str, dict[str, int]]:
    if (
        not isinstance(scanner_artifact_by_field, Mapping)
        or set(scanner_artifact_by_field) != _PTG2_TAX_IDENTITY_FRAME_FIELDS
    ):
        raise RuntimeError(
            "PTG V4 scanner omitted provider-group tax identity evidence"
        )
    expected_path = artifact_path.resolve()
    reported_path = Path(str(scanner_artifact_by_field.get("path") or "")).resolve()
    policy_id = scanner_artifact_by_field.get("token_policy_id")
    digest = str(scanner_artifact_by_field.get("sha256") or "").lower()
    count_names = (
        "row_count",
        "provider_group_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
    )
    count_by_name = {name: scanner_artifact_by_field.get(name) for name in count_names}
    if (
        reported_path != expected_path
        or scanner_artifact_by_field.get("format")
        != "ptg2_provider_group_tax_identity_v1"
        or scanner_artifact_by_field.get("version") != _PTG2_TAX_IDENTITY_VERSION
        or scanner_artifact_by_field.get("record_bytes")
        != _PTG2_TAX_IDENTITY_RECORD_BYTES
        or scanner_artifact_by_field.get("normalization_contract")
        != "ein_ascii_digits_or_2_7_hyphen_v1"
        or scanner_artifact_by_field.get("hmac_contract") != "hmac_sha256_ptg_tin_v1"
        or scanner_artifact_by_field.get("final") is not True
        or not isinstance(policy_id, str)
        or len(policy_id.encode("ascii", errors="ignore")) != len(policy_id)
        or len(policy_id.encode("ascii")) > 55
        or _PTG2_TAX_IDENTITY_POLICY_ID_RE.fullmatch(policy_id) is None
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or any(
            type(count_value) is not int or count_value < 0
            for count_value in count_by_name.values()
        )
    ):
        raise RuntimeError("PTG V4 provider-group tax identity evidence is invalid")
    return policy_id, digest, count_by_name


def _validate_tax_identity_artifact_content(
    artifact_path: Path,
    scanner_artifact_by_field: Mapping[str, Any],
    policy_id: str,
    digest: str,
    count_by_name: dict[str, int],
) -> int:
    row_count = count_by_name["row_count"]
    if (
        count_by_name["provider_group_count"] != row_count
        or sum(
            count_by_name[name]
            for name in (
                "matched_ein_count",
                "missing_count",
                "malformed_count",
                "unsupported_type_count",
            )
        )
        != row_count
    ):
        raise RuntimeError("PTG V4 provider-group tax identity counts are inconsistent")
    byte_count = artifact_path.stat().st_size
    policy_bytes = policy_id.encode("ascii")
    expected_bytes = (
        13 + len(policy_bytes) + row_count * _PTG2_TAX_IDENTITY_RECORD_BYTES
    )
    if (
        scanner_artifact_by_field.get("bytes") != byte_count
        or byte_count != expected_bytes
    ):
        raise RuntimeError("PTG V4 provider-group tax identity artifact size changed")
    with artifact_path.open("rb") as artifact_file:
        header = artifact_file.read(13 + len(policy_bytes))
    if (
        len(header) != 13 + len(policy_bytes)
        or header[:8] != _PTG2_TAX_IDENTITY_MAGIC
        or int.from_bytes(header[8:10], "little") != _PTG2_TAX_IDENTITY_VERSION
        or int.from_bytes(header[10:12], "little") != _PTG2_TAX_IDENTITY_RECORD_BYTES
        or header[12] != len(policy_bytes)
        or header[13:] != policy_bytes
    ):
        raise RuntimeError(
            "PTG V4 provider-group tax identity artifact header is invalid"
        )
    actual_digest, actual_bytes = sha256_file(artifact_path)
    if actual_digest != digest or actual_bytes != byte_count:
        raise RuntimeError("PTG V4 provider-group tax identity artifact digest changed")
    return byte_count


def _validated_provider_group_tax_identity_artifact(
    artifact_path: Path,
    scanner_artifact_by_field: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Authenticate the scanner's token-only tax-identity artifact summary."""

    policy_id, digest, count_by_name = _validate_tax_identity_summary_frame(
        artifact_path,
        scanner_artifact_by_field,
    )
    assert isinstance(scanner_artifact_by_field, Mapping)
    byte_count = _validate_tax_identity_artifact_content(
        artifact_path,
        scanner_artifact_by_field,
        policy_id,
        digest,
        count_by_name,
    )
    return {
        "name": "provider_group_tax_identity",
        "path": str(artifact_path),
        "record_format": scanner_artifact_by_field["format"],
        "sha256": digest,
        "byte_count": byte_count,
        **{
            field_name: scanner_artifact_by_field[field_name]
            for field_name in _PTG2_TAX_IDENTITY_FRAME_FIELDS
            if field_name not in {"path", "bytes", "format", "sha256"}
        },
    }


def _ptg2_existing_manifest_copy_paths(input_paths: list[Path]) -> list[Path]:
    return [path for path in input_paths if path.exists() and path.stat().st_size > 0]


def _ptg2_provider_membership_sidecar_command(
    *,
    provider_group_npi_path: Path,
    provider_npi_group_path: Path,
    provider_npi_scope_copy_path: Path,
    input_paths: list[Path],
) -> list[str]:
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 provider membership sidecars require the Rust scanner binary; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    return [
        str(binary),
        "--provider-membership-sidecars",
        str(provider_group_npi_path),
        str(provider_npi_group_path),
        str(provider_npi_scope_copy_path),
        *[str(path) for path in input_paths],
    ]


async def _build_ptg2_provider_membership_sidecars(
    *,
    provider_group_npi_path: Path,
    provider_npi_group_path: Path,
    provider_npi_scope_copy_path: Path,
    input_paths: list[Path],
) -> dict[str, Any]:
    existing_paths = _ptg2_existing_manifest_copy_paths(input_paths)
    command = _ptg2_provider_membership_sidecar_command(
        provider_group_npi_path=provider_group_npi_path,
        provider_npi_group_path=provider_npi_group_path,
        provider_npi_scope_copy_path=provider_npi_scope_copy_path,
        input_paths=existing_paths,
    )

    def _invoke_scanner() -> subprocess.CompletedProcess[bytes]:
        """Run the scanner without blocking the import event loop."""
        return subprocess.run(command, check=True, capture_output=True)

    completed = await asyncio.to_thread(_invoke_scanner)
    try:
        header, rest = completed.stdout.split(b"\n", 1)
        record_kind, length_bytes = header.split(b"\t", 1)
        summary_json = rest[: int(length_bytes)]
        if record_kind != b"provider_membership_sidecars":
            raise ValueError(f"unexpected record kind: {record_kind!r}")
        return json.loads(summary_json)
    except Exception as exc:
        raise RuntimeError(
            "PTG2 provider membership sidecar builder returned invalid output"
        ) from exc


def _emit_ptg2_publish_progress(
    publish_step: str,
    *,
    completed_steps: int,
    total_steps: int,
    message_text: str | None = None,
    stage_start_pct: float = 92.0,
    stage_end_pct: float = 99.0,
    **progress_details: Any,
) -> None:
    total_steps = max(int(total_steps or 1), 1)
    completed_steps = max(0, min(int(completed_steps), total_steps))
    phase_pct = (completed_steps / total_steps) * 100.0
    progress_pct = _scale_stage_progress_pct(phase_pct, stage_start_pct, stage_end_pct)
    progress_message = message_text or f"publishing {publish_step}"
    progress_payload_dict = {
        "phase": f"publishing: {publish_step}"[:128],
        "unit": "publish_steps",
        "done": completed_steps,
        "total": total_steps,
        "pct": progress_pct,
        "phase_pct": phase_pct,
        "message": progress_message,
        "detail": progress_message,
        "source": "ptg2-publish-progress",
        "confidence": "live",
        "publish_step": publish_step,
        **{
            detail_key: detail_value
            for detail_key, detail_value in progress_details.items()
            if detail_value is not None
        },
    }
    try:
        write_live_progress(**progress_payload_dict)
    except Exception:
        logger.debug("Failed to write PTG2 publish live progress", exc_info=True)


class _PTG2V4PublicationProgress:
    """Expose measured V4 publication work without hiding compiler progress."""

    _PCT_LOWER_BOUND_BY_STAGE = {
        "price preparation and publication": 91.25,
        "finalizer": 91.25,
        "price key map export": 91.5,
        "dictionary publication": 91.5,
        "provider set key export": 91.75,
        "provider graph conversion": 95.0,
        "serving block publication": 95.25,
        "provider graph publication": 95.25,
        "price publication": 95.25,
        "source witness publication": 95.25,
        "mapping summary": 96.0,
        "audit publication": 96.5,
        "snapshot seal": 97.0,
    }
    _POST_COMPILE_STAGES = frozenset(
        stage_name
        for stage_name, pct_lower_bound in _PCT_LOWER_BOUND_BY_STAGE.items()
        if pct_lower_bound >= 95.0
    )

    def __init__(self) -> None:
        self._event_count = 0
        self._pct_lower_bound = 91.25
        self._post_compile = False
        self._counters_by_stage: dict[str, dict[str, int]] = {}

    def _retain_counters(
        self,
        stage_name: str,
        counters_by_name: Mapping[str, int],
    ) -> dict[str, int]:
        retained_by_name = self._counters_by_stage.setdefault(stage_name, {})
        for raw_name, raw_counter in counters_by_name.items():
            counter_value = int(raw_counter)
            if counter_value >= 0:
                counter_name = str(raw_name)
                retained_by_name[counter_name] = max(
                    retained_by_name.get(counter_name, 0),
                    counter_value,
                )
        aggregate_by_name: dict[str, int] = {}
        for stage_counters in self._counters_by_stage.values():
            for counter_name, counter_value in stage_counters.items():
                aggregate_by_name[counter_name] = (
                    aggregate_by_name.get(counter_name, 0) + counter_value
                )
        return aggregate_by_name

    def observe(
        self,
        stage_name: str,
        counters_by_name: Mapping[str, int],
    ) -> None:
        """Persist one exact publication observation under the proper stage fence."""

        normalized_stage_name = str(stage_name or "publication").strip()
        normalized_stage_name = normalized_stage_name or "publication"
        aggregate_by_name = self._retain_counters(
            normalized_stage_name,
            counters_by_name,
        )
        self._event_count += 1
        self._post_compile = (
            self._post_compile or normalized_stage_name in self._POST_COMPILE_STAGES
        )
        self._pct_lower_bound = max(
            self._pct_lower_bound,
            self._PCT_LOWER_BOUND_BY_STAGE.get(
                normalized_stage_name,
                self._pct_lower_bound,
            ),
        )
        aggregate_by_name["publication_progress_events"] = self._event_count
        self._write_progress(normalized_stage_name, aggregate_by_name)

    def _write_progress(
        self,
        stage_name: str,
        counters_by_name: Mapping[str, int],
    ) -> None:
        """Write the current measured lower bound without inventing a total."""

        progress_message = (
            f"publishing PTG snapshot: {stage_name}; "
            f"measured events={self._event_count}"
        )
        try:
            write_live_progress(
                phase=f"publishing: snapshot {stage_name}"[:128],
                stage_id=(
                    "ptg2_v4_publication"
                    if self._post_compile
                    else "ptg2_v4_precompile"
                ),
                stage_ordinal=6 if self._post_compile else 4,
                unit="publication_events",
                basis="semantic_work",
                denominator_state="lower_bound",
                done=self._event_count,
                total=None,
                work_done=self._event_count,
                work_total=None,
                pct=None,
                pct_lower_bound=self._pct_lower_bound,
                phase_pct=None,
                stage_pct=None,
                eta_seconds=None,
                message=progress_message,
                detail=progress_message,
                source="ptg2-v4-publication-progress",
                confidence="measured",
                publication_stage=stage_name,
                counters=dict(counters_by_name),
            )
        except Exception:
            logger.debug(
                "Failed to write measured PTG V4 publication progress",
                exc_info=True,
            )


def _copy_file_row_count(copy_file_entry: dict[str, Any]) -> int:
    try:
        return int(copy_file_entry.get("row_count") or 0)
    except (TypeError, ValueError):
        return 0


def _collect_manifest_copy_files(
    successful_files: list[dict[str, Any]],
    copy_kinds: list[str],
) -> tuple[dict[str, list[Path]], dict[str, int]]:
    copy_files_by_kind: dict[str, list[Path]] = {kind: [] for kind in copy_kinds}
    emitted_rows_by_kind: dict[str, int] = {kind: 0 for kind in copy_kinds}
    for file_summary in successful_files:
        summary_payload = (
            file_summary.get("summary") if isinstance(file_summary, dict) else None
        )
        manifest_payload = (
            summary_payload.get("manifest")
            if isinstance(summary_payload, dict)
            else None
        )
        copy_files = (
            manifest_payload.get("copy_files")
            if isinstance(manifest_payload, dict)
            else None
        )
        if not isinstance(copy_files, dict):
            continue
        for kind in copy_kinds:
            for copy_file_entry in copy_files.get(kind) or []:
                if not isinstance(copy_file_entry, dict):
                    continue
                raw_path = str(copy_file_entry.get("path") or "").strip()
                if not raw_path:
                    continue
                copy_files_by_kind[kind].append(Path(raw_path))
                emitted_rows_by_kind[kind] += _copy_file_row_count(copy_file_entry)
    return copy_files_by_kind, emitted_rows_by_kind


def _count_manifest_copy_sources(
    successful_files: list[dict[str, Any]],
    copy_kinds: Sequence[str],
    *,
    require_complete_sources: bool = False,
) -> dict[str, int]:
    """Count logical scanner sources, independent of worker/rotation shards."""

    ordered_kinds = tuple(copy_kinds)
    required_kinds = set(ordered_kinds)
    source_count_by_kind = {kind: 0 for kind in ordered_kinds}
    for file_index, file_summary in enumerate(successful_files):
        summary_payload, manifest_payload = _manifest_summary_payloads(file_summary)
        copy_files_by_kind = (
            manifest_payload.get("copy_files")
            if isinstance(manifest_payload, dict)
            else None
        )
        if not isinstance(copy_files_by_kind, dict):
            copy_files_by_kind = {}
        present_kinds: set[str] = set()
        for kind in ordered_kinds:
            if any(
                isinstance(entry, dict) and str(entry.get("path") or "").strip()
                for entry in (copy_files_by_kind.get(kind) or ())
            ):
                present_kinds.add(kind)
                source_count_by_kind[kind] += 1
        serving_rows = _manifest_serving_row_count(
            summary_payload,
            manifest_payload,
        )
        if (
            require_complete_sources
            and (present_kinds or serving_rows > 0)
            and present_kinds != required_kinds
        ):
            source_label = str(
                file_summary.get("url")
                or file_summary.get("file_id")
                or f"index {file_index}"
            )
            missing_kinds = sorted(required_kinds - present_kinds)
            raise RuntimeError(
                "strict V3 scanner source "
                f"{source_label!r} omitted required price COPY artifacts: "
                + ", ".join(missing_kinds)
            )
    return source_count_by_kind


def _manifest_summary_payloads(
    file_summary: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    summary_payload = file_summary.get("summary")
    if not isinstance(summary_payload, Mapping):
        return {}, {}
    manifest_payload = summary_payload.get("manifest")
    return (
        summary_payload,
        manifest_payload if isinstance(manifest_payload, Mapping) else {},
    )


def _manifest_serving_row_count(
    summary_payload: Mapping[str, Any],
    manifest_payload: Mapping[str, Any],
) -> int:
    try:
        return int(
            manifest_payload.get("serving_rows")
            or summary_payload.get("serving_rates")
            or 0
        )
    except (TypeError, ValueError):
        return 0


def _collect_manifest_copy_entries(
    successful_files: list[dict[str, Any]],
    copy_kinds: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """Collect metadata-bearing deferred files without opening their payloads."""

    entries_by_kind: dict[str, list[dict[str, Any]]] = {kind: [] for kind in copy_kinds}
    seen_paths_by_kind: dict[str, set[str]] = {kind: set() for kind in copy_kinds}
    for file_summary in successful_files:
        summary_payload = (
            file_summary.get("summary") if isinstance(file_summary, dict) else None
        )
        manifest_payload = (
            summary_payload.get("manifest")
            if isinstance(summary_payload, dict)
            else None
        )
        copy_files = (
            manifest_payload.get("copy_files")
            if isinstance(manifest_payload, dict)
            else None
        )
        if not isinstance(copy_files, dict):
            continue
        for kind in copy_kinds:
            for raw_entry in copy_files.get(kind) or ():
                if not isinstance(raw_entry, dict):
                    continue
                path = str(raw_entry.get("path") or "").strip()
                if not path or path in seen_paths_by_kind[kind]:
                    continue
                seen_paths_by_kind[kind].add(path)
                entries_by_kind[kind].append(dict(raw_entry))
    return entries_by_kind


def _pending_strict_v3_copy_entries(
    successful_files: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Register every strict-V3 scratch file for import-level failure cleanup."""

    return _collect_manifest_copy_entries(
        successful_files,
        (
            "serving_run",
            "serving_code_dictionary",
            "source_audit_witness",
            "provider_set_metadata",
            "price_atom",
            "price_set_atom",
            "price_set_summary",
        ),
    )


@dataclass
class _ManifestCopyProgress:
    kind: str
    target_table: str
    completed_steps_before_copy: int
    total_steps: int
    input_file_count: int
    input_bytes: int
    started_at: float
    progress_by_field: dict[str, Any]
    lock: threading.Lock

    def _report_copied_bytes(self, byte_count: int) -> None:
        if byte_count <= 0:
            return
        with self.lock:
            self.progress_by_field["copied_bytes"] += int(byte_count)
            now = _ptg2_monotonic()
            if (
                self.progress_by_field["copied_bytes"]
                <= self.progress_by_field["last_emitted_bytes"]
                or now < self.progress_by_field["next_progress_at"]
            ):
                return
            copied_bytes = self.progress_by_field["copied_bytes"]
            self.progress_by_field["last_emitted_bytes"] = copied_bytes
            self.progress_by_field["next_progress_at"] = (
                now + _PTG2_PUBLISH_PROGRESS_INTERVAL_SECONDS
            )
            elapsed_seconds = max(now - self.started_at, 0.0)
            _emit_ptg2_publish_progress(
                f"copying {self.kind}",
                completed_steps=self.completed_steps_before_copy,
                total_steps=self.total_steps,
                stage_start_pct=92.0,
                stage_end_pct=95.0,
                message_text=(
                    f"copied {copied_bytes} of {self.input_bytes} "
                    f"{self.kind} byte(s) into {self.target_table}"
                ),
                copy_kind=self.kind,
                target_table=self.target_table,
                input_files=self.input_file_count,
                input_bytes=self.input_bytes,
                direct_to_copy=True,
                counters={
                    "manifest_copy_bytes": copied_bytes,
                    "manifest_copy_total_bytes": self.input_bytes,
                },
                throughput={
                    "bytes_per_second": (
                        copied_bytes / elapsed_seconds if elapsed_seconds > 0 else None
                    )
                },
            )


def _manifest_copy_task_count() -> int:
    return max(
        _env_int(
            PTG2_MANIFEST_DIRECT_COPY_TASKS_ENV,
            PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS,
        ),
        1,
    )


def _emit_manifest_copy_start(
    progress: _ManifestCopyProgress,
    *,
    emitted_rows: int | None,
    copy_tasks: int,
) -> None:
    _emit_ptg2_publish_progress(
        f"copying {progress.kind}",
        completed_steps=progress.completed_steps_before_copy,
        total_steps=progress.total_steps,
        stage_start_pct=92.0,
        stage_end_pct=95.0,
        message_text=(
            f"copying {progress.kind} worker files into " f"{progress.target_table}"
        ),
        copy_kind=progress.kind,
        target_table=progress.target_table,
        input_files=progress.input_file_count,
        emitted_rows=emitted_rows,
        direct_to_copy=True,
        copy_tasks=min(copy_tasks, max(progress.input_file_count, 1)),
    )


async def _copy_one_manifest_path(
    input_path: Path,
    *,
    target_table: str,
    copy_func: Any,
    progress_callback: Callable[[int], None],
    semaphore: asyncio.Semaphore | None = None,
) -> None:
    if semaphore is None:
        await copy_func(
            input_path,
            target_table=target_table,
            progress_callback=progress_callback,
        )
        return
    async with semaphore:
        await copy_func(
            input_path,
            target_table=target_table,
            progress_callback=progress_callback,
        )


async def _copy_manifest_paths(
    input_paths: Sequence[Path],
    *,
    target_table: str,
    copy_func: Any,
    progress_callback: Callable[[int], None],
    copy_tasks: int,
) -> None:
    semaphore = (
        asyncio.Semaphore(copy_tasks)
        if copy_tasks > 1 and len(input_paths) > 1
        else None
    )
    if semaphore is None:
        for input_path in input_paths:
            await _copy_one_manifest_path(
                input_path,
                target_table=target_table,
                copy_func=copy_func,
                progress_callback=progress_callback,
            )
        return
    await asyncio.gather(
        *(
            _copy_one_manifest_path(
                input_path,
                target_table=target_table,
                copy_func=copy_func,
                progress_callback=progress_callback,
                semaphore=semaphore,
            )
            for input_path in input_paths
        )
    )


def _completed_manifest_copy_result(
    progress: _ManifestCopyProgress,
    *,
    emitted_rows: int | None,
    copy_tasks: int,
) -> dict[str, Any]:
    elapsed_seconds = _ptg2_monotonic() - progress.started_at
    row_count = int(emitted_rows or 0)
    bounded_copy_tasks = min(
        copy_tasks,
        max(progress.input_file_count, 1),
    )
    _emit_ptg2_publish_progress(
        f"copied {progress.kind}",
        completed_steps=progress.completed_steps_before_copy + 1,
        total_steps=progress.total_steps,
        stage_start_pct=92.0,
        stage_end_pct=95.0,
        message_text=(
            f"copied {row_count} {progress.kind} row(s) into "
            f"{progress.target_table}"
        ),
        copy_kind=progress.kind,
        target_table=progress.target_table,
        input_files=progress.input_file_count,
        input_bytes=progress.input_bytes,
        input_rows=row_count,
        output_rows=row_count,
        dropped_rows=0,
        counters={
            "manifest_copy_bytes": progress.progress_by_field["copied_bytes"],
            "manifest_copy_total_bytes": progress.input_bytes,
            "manifest_copy_rows": row_count,
        },
        direct_to_copy=True,
        copy_tasks=bounded_copy_tasks,
        elapsed_seconds=elapsed_seconds,
    )
    return {
        "kind": progress.kind,
        "input_files": progress.input_file_count,
        "input_bytes": progress.input_bytes,
        "input_rows": row_count,
        "output_rows": row_count,
        "dropped_rows": 0,
        "direct_to_copy": True,
        "copy_tasks": bounded_copy_tasks,
        "elapsed_seconds": elapsed_seconds,
        "rows_per_second": (
            row_count / elapsed_seconds if elapsed_seconds > 0 else None
        ),
        "bytes_per_second": (
            progress.input_bytes / elapsed_seconds if elapsed_seconds > 0 else None
        ),
    }


async def _copy_manifest_files_direct_with_progress(
    kind: str,
    *,
    target_table: str,
    input_paths: list[Path],
    copy_func,
    completed_steps_before_copy: int,
    total_steps: int,
    emitted_rows: int | None,
) -> dict[str, Any]:
    """Copy manifest worker files, emit progress, and return throughput metrics."""

    existing_paths = _ptg2_existing_manifest_copy_paths(input_paths)
    input_bytes = sum(input_path.stat().st_size for input_path in existing_paths)
    copy_started_at = _ptg2_monotonic()
    progress = _ManifestCopyProgress(
        kind=kind,
        target_table=target_table,
        completed_steps_before_copy=completed_steps_before_copy,
        total_steps=total_steps,
        input_file_count=len(existing_paths),
        input_bytes=input_bytes,
        started_at=copy_started_at,
        progress_by_field={
            "copied_bytes": 0,
            "last_emitted_bytes": 0,
            "next_progress_at": (
                copy_started_at + _PTG2_PUBLISH_PROGRESS_INTERVAL_SECONDS
            ),
        },
        lock=threading.Lock(),
    )
    copy_tasks = _manifest_copy_task_count()
    _emit_manifest_copy_start(
        progress,
        emitted_rows=emitted_rows,
        copy_tasks=copy_tasks,
    )
    await _copy_manifest_paths(
        existing_paths,
        target_table=target_table,
        copy_func=copy_func,
        progress_callback=progress._report_copied_bytes,
        copy_tasks=copy_tasks,
    )
    return _completed_manifest_copy_result(
        progress,
        emitted_rows=emitted_rows,
        copy_tasks=copy_tasks,
    )


def _cleanup_manifest_copy_paths(copy_files_by_kind: dict[str, list[Path]]) -> None:
    for copy_file_paths in copy_files_by_kind.values():
        for copy_file_path in copy_file_paths:
            base_copy_path = _manifest_copy_base_path(copy_file_path)
            try:
                copy_file_path.unlink(missing_ok=True)
            except Exception:
                logger.debug(
                    "Failed to remove PTG2 manifest merge file %s",
                    copy_file_path,
                    exc_info=True,
                )
            _cleanup_empty_manifest_copy_siblings(base_copy_path)


def _cleanup_manifest_copy_entries(
    copy_entries_by_kind: Mapping[str, Sequence[Mapping[str, Any]]],
) -> None:
    paths_by_kind = {
        str(kind): [
            Path(str(entry.get("path"))) for entry in entries if entry.get("path")
        ]
        for kind, entries in copy_entries_by_kind.items()
    }
    run_directories = {
        path.parent
        for paths in paths_by_kind.values()
        for path in paths
        if path.parent.name.startswith("ptg2-v3-runs-")
    }
    _cleanup_manifest_copy_paths(paths_by_kind)
    for run_directory in run_directories:
        shutil.rmtree(run_directory, ignore_errors=True)


def _cleanup_strict_v3_graph_artifacts(artifacts: Mapping[str, Any]) -> None:
    """Remove import-only graph files after they are durable in PostgreSQL."""

    artifact_root = (resolve_ptg2_artifact_dir() / "serving").resolve()
    parent_directories: set[Path] = set()
    for entry in artifacts.get("sidecars") or ():
        if not isinstance(entry, Mapping):
            continue
        raw_path = str(entry.get("path") or "").strip()
        if not raw_path or "://" in raw_path:
            continue
        path = Path(raw_path).resolve()
        try:
            path.relative_to(artifact_root)
        except ValueError:
            logger.warning(
                "Refusing to remove PTG graph artifact outside %s: %s",
                artifact_root,
                path,
            )
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning(
                "Failed to remove imported PTG graph artifact %s", path, exc_info=True
            )
            continue
        parent_directories.add(path.parent)
    for directory in sorted(
        parent_directories, key=lambda value: len(value.parts), reverse=True
    ):
        current = directory
        while current != artifact_root:
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent


async def _cancel_and_wait_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    """Cancel child work and wait until it can no longer use import inputs."""

    remaining_tasks = tuple(tasks)
    for task in remaining_tasks:
        task.cancel()
    if remaining_tasks:
        await asyncio.gather(*remaining_tasks, return_exceptions=True)
    tasks.clear()


@asynccontextmanager
async def _ptg2_source_import_lock(source_key: str):
    """Serialize full imports for one source without holding a SQL transaction."""

    if not _env_bool(PTG2_SOURCE_IMPORT_LOCK_ENABLED_ENV, True):
        yield
        return
    if db.engine is None:
        await db.connect()
    assert db.engine is not None
    lock_name = f"ptg2_source_import_v1:{source_key}"
    async with db.engine.connect() as connection:
        while True:
            lock_query_result = await connection.execute(
                db.text("SELECT pg_try_advisory_lock(hashtextextended(:lock_name, 0))"),
                {"lock_name": lock_name},
            )
            acquired = bool(lock_query_result.scalar())
            await connection.commit()
            if acquired:
                break
            write_live_progress(
                phase="waiting for source import",
                pct=1,
                message="waiting for another import of this source to finish",
            )
            await asyncio.sleep(5)
        try:
            yield
        finally:
            await connection.execute(
                db.text("SELECT pg_advisory_unlock(hashtextextended(:lock_name, 0))"),
                {"lock_name": lock_name},
            )
            await connection.commit()


def _manifest_copy_base_path(copy_file_path: Path) -> Path:
    name = copy_file_path.name
    copy_suffix_index = name.find(".copy")
    if copy_suffix_index < 0:
        return copy_file_path
    return copy_file_path.with_name(name[: copy_suffix_index + len(".copy")])


def _cleanup_empty_manifest_copy_siblings(copy_path: Path) -> None:
    for pattern in (
        f"{copy_path.name}.worker*",
        f"{copy_path.name}.provider_refs.worker*",
    ):
        for worker_copy_path in copy_path.parent.glob(pattern):
            try:
                if worker_copy_path.is_file() and worker_copy_path.stat().st_size == 0:
                    worker_copy_path.unlink(missing_ok=True)
            except Exception:
                logger.debug(
                    "Failed to remove empty PTG2 manifest worker copy file %s",
                    worker_copy_path,
                    exc_info=True,
                )


def _cleanup_manifest_copy_family(copy_path: Path) -> None:
    for family_path in (copy_path, *copy_path.parent.glob(f"{copy_path.name}*")):
        try:
            if family_path.is_file():
                family_path.unlink(missing_ok=True)
        except Exception:
            logger.debug(
                "Failed to remove PTG2 manifest copy file %s",
                family_path,
                exc_info=True,
            )


async def _merge_ptg2_manifest_files(
    *,
    successful_files: list[dict[str, Any]],
    manifest_stage_table: str,
) -> dict[str, Any]:
    """Merge validated per-source price COPY families into the shared stage."""

    if _is_postgres_binary_v3_arch(_ptg2_snapshot_arch_from_env()):
        copy_kinds = ("price_atom", "price_set_atom", "price_set_summary")
        copy_files_by_kind, emitted_rows_by_kind = _collect_manifest_copy_files(
            successful_files,
            list(copy_kinds),
        )
        try:
            source_files_by_kind = _count_manifest_copy_sources(
                successful_files,
                copy_kinds,
                require_complete_sources=True,
            )
            return await _copy_strict_v3_price_files(
                copy_kinds,
                copy_files_by_kind,
                emitted_rows_by_kind,
                source_files_by_kind,
                manifest_stage_table,
            )
        finally:
            _cleanup_manifest_copy_paths(copy_files_by_kind)


async def _copy_strict_v3_price_files(
    copy_kinds: tuple[str, ...],
    copy_files_by_kind: dict[str, list[Path]],
    emitted_rows_by_kind: dict[str, int],
    source_files_by_kind: dict[str, int],
    manifest_stage_table: str,
) -> dict[str, Any]:
    if len(set(source_files_by_kind.values())) > 1:
        raise RuntimeError(
            "strict V3 price COPY artifact source counts disagree: "
            + json.dumps(source_files_by_kind, sort_keys=True)
        )
    if not any(copy_files_by_kind.values()):
        return {
            "enabled": False,
            "reason": "no_strict_v3_price_copy_files",
            "source_files_by_kind": source_files_by_kind,
        }
    missing_kinds = [kind for kind in copy_kinds if not copy_files_by_kind[kind]]
    if missing_kinds:
        raise RuntimeError(
            "strict V3 scanner omitted required price COPY artifacts: "
            + ", ".join(missing_kinds)
        )
    target_by_kind = {
        kind: _ptg2_manifest_support_stage_table(manifest_stage_table, kind)
        for kind in copy_kinds
    }
    copy_func_by_kind = {
        "price_atom": _copy_price_atom_file,
        "price_set_atom": _copy_price_atom_member_file,
        "price_set_summary": _copy_price_set_summary_file,
    }
    copy_report_map: dict[str, Any] = {
        "enabled": True,
        "strict_v3_price_only": True,
        "kinds": {},
        "emitted_rows": emitted_rows_by_kind,
        "source_files_by_kind": source_files_by_kind,
    }
    active_kinds = [kind for kind in copy_kinds if copy_files_by_kind[kind]]
    for completed_steps, kind in enumerate(active_kinds):
        copy_report_map["kinds"][kind] = (
            await _copy_manifest_files_direct_with_progress(
                kind,
                target_table=target_by_kind[kind],
                input_paths=copy_files_by_kind[kind],
                copy_func=copy_func_by_kind[kind],
                completed_steps_before_copy=completed_steps,
                total_steps=max(len(active_kinds), 1),
                emitted_rows=emitted_rows_by_kind.get(kind),
            )
        )
    copy_report_map["direct_to_copy"] = True
    _emit_screen_line(
        "PTG2_STRICT_V3_PRICE_COPY\t" f"{json.dumps(copy_report_map, sort_keys=True)}"
    )
    return copy_report_map


def _record_v3_scanner_summary(
    scanner_summary_by_name: Mapping[str, Any],
    deferred_copy_entries_by_kind: dict[str, list[dict[str, Any]]],
    row_counts_by_name: dict[str, int],
) -> None:
    copy_file_field_by_kind = {
        "serving_run": "serving_run_partition_files",
        "serving_code_dictionary": "serving_run_code_dictionary_files",
    }
    for copy_kind, field_name in copy_file_field_by_kind.items():
        candidate_entries = scanner_summary_by_name.get(field_name)
        if not isinstance(candidate_entries, list):
            continue
        deferred_copy_entries_by_kind[copy_kind].extend(
            dict(copy_entry)
            for copy_entry in candidate_entries
            if isinstance(copy_entry, dict)
        )
    row_counts_by_name["serving"] = int(
        scanner_summary_by_name.get("serving_run_rows") or 0
    )


async def _parse_strict_v3_file(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    source_version: PTG2SourceVersion | None,
    snapshot_id: str,
    coverage_scope_id: str,
    import_month: datetime.date,
    max_items: int | None = None,
    ptg2_manifest_stage_table: str | None = None,
    source_network_names: list[str] | str | None = None,
    progress_observer: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Scan one file into strict V3 COPY artifacts and clean incomplete scratch state."""

    if not ptg2_manifest_stage_table:
        raise RuntimeError("PTG imports require manifest serving stage tables")
    if max_items is not None:
        logger.info(
            "Ignoring max_items=%s for manifest-backed Rust PTG import", max_items
        )

    plan_fields = _derive_plan_fields(meta, plan_info)
    source_network_name_values = _normalize_source_network_names(source_network_names)
    arch_version = _ptg2_snapshot_arch_from_env()
    if not _is_postgres_binary_v3_arch(arch_version):
        raise RuntimeError("only postgres_binary_v3 PTG imports are supported")
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(
        plan_fields, snapshot_id, import_month
    )
    _source_trace_row, _source_trace_set_row = _ptg2_source_trace_rows(
        source_version, source_url
    )
    source_trace_hash = _source_trace_row["source_trace_hash"]
    source_trace_set_hash = _source_trace_set_row["source_trace_set_hash"]

    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)

    copy_tmp_dir = ptg2_temp_parent()
    manifest_copy_row_counter_by_name = {"serving": 0}
    rust_records = 0
    rust_dedupe_summary_by_field: dict[str, Any] = {}
    rust_scanner_config_by_name: dict[str, Any] = {}
    rust_scanner_summary_by_name: dict[str, Any] = {}
    provider_group_tax_identity_artifact_by_field: dict[str, Any] | None = None
    procedure_hashes: set[str] = set()
    deferred_copy_entries_by_kind: dict[str, list[dict[str, Any]]] = {
        "serving_run": [],
        "serving_code_dictionary": [],
        "source_audit_witness": [],
        "price_atom": [],
        "price_set_atom": [],
        "price_set_summary": [],
        "provider_group_member": [],
        "provider_set_metadata": [],
    }
    deferred_copy_file_paths_by_kind: dict[str, set[str]] = {
        kind: set() for kind in deferred_copy_entries_by_kind
    }
    manifest_copy_file_accounting_by_name = {
        "scanner_reported_files": 0,
        "scanner_duplicate_files": 0,
        "recovery_candidates": 0,
        "recovery_already_reported_files": 0,
        "recovered_unreported_files": 0,
        "fallback_row_count_files": 0,
        "fallback_row_count_bytes": 0,
    }
    is_scan_complete = False

    def _new_copy_path(prefix: str) -> Path:
        fd, name = tempfile.mkstemp(prefix=prefix, suffix=".copy", dir=copy_tmp_dir)
        os.close(fd)
        return Path(name)

    def _copy_file_key(copy_file: Path) -> str:
        try:
            return str(copy_file.resolve())
        except Exception:
            return str(copy_file)

    def _record_deferred_copy_file_once(
        kind: str,
        copy_file: Path,
        row_count: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> int:
        path_key = _copy_file_key(copy_file)
        seen_paths = deferred_copy_file_paths_by_kind.setdefault(kind, set())
        if path_key in seen_paths:
            return 0
        seen_paths.add(path_key)
        copy_entry_by_field: dict[str, Any] = {
            "path": str(copy_file),
            "row_count": row_count,
        }
        if kind == "provider_set_metadata":
            file_size = copy_file.stat().st_size
            expected_size = int((metadata or {}).get("bytes") or 0)
            if expected_size > 0 and expected_size != file_size:
                raise RuntimeError(
                    "strict V3 provider-set metadata size changed after scanner close"
                )
            digest = hashlib.sha256()
            with copy_file.open("rb") as source_stream:
                while chunk := source_stream.read(1024 * 1024):
                    digest.update(chunk)
            copy_entry_by_field.update(
                {
                    "bytes": file_size,
                    "sha256": digest.hexdigest(),
                    "format": "ptg2_v3_provider_set_metadata_copy",
                    "version": 1,
                    "final": bool((metadata or {}).get("final", True)),
                }
            )
        deferred_copy_entries_by_kind[kind].append(copy_entry_by_field)
        return row_count

    def _manifest_copy_candidates(copy_path: Path) -> list[Path]:
        candidate_paths: list[Path] = []
        seen_paths: set[str] = set()
        if copy_path.exists():
            candidate_paths.append(copy_path)
            seen_paths.add(_copy_file_key(copy_path))
        for worker_copy_path in sorted(copy_path.parent.glob(f"{copy_path.name}*")):
            if not worker_copy_path.is_file():
                continue
            path_key = _copy_file_key(worker_copy_path)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
            candidate_paths.append(worker_copy_path)
        return candidate_paths

    manifest_price_atom_copy_path = _new_copy_path("ptg2_manifest_price_atom_")
    manifest_price_set_atom_copy_path = _new_copy_path("ptg2_manifest_price_set_atom_")
    manifest_price_set_summary_copy_path = _new_copy_path(
        "ptg2_manifest_price_set_summary_"
    )
    manifest_provider_group_member_copy_path = _new_copy_path(
        "ptg2_manifest_provider_group_member_"
    )
    manifest_provider_set_metadata_copy_path = _new_copy_path(
        "ptg2_v3_provider_set_metadata_"
    )
    v3_serving_run_directory = Path(
        tempfile.mkdtemp(prefix="ptg2-v3-runs-", dir=copy_tmp_dir)
    )
    manifest_file_token = hashlib.sha256(
        str(Path(file_path).resolve()).encode("utf-8")
    ).hexdigest()[:16]
    manifest_artifact_parent = resolve_ptg2_artifact_dir() / "serving"
    manifest_artifact_parent.mkdir(parents=True, exist_ok=True)
    manifest_artifact_dir = Path(
        tempfile.mkdtemp(
            prefix=(
                f"{_ptg2_snapshot_table_token(str(plan_fields.get('plan_id') or 'plan'), snapshot_id)}-"
                f"{manifest_file_token}-"
            ),
            dir=manifest_artifact_parent,
        )
    )
    provider_graph_v4 = _env_bool("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", False)
    manifest_sidecar_paths_by_kind = {
        "provider_forward": (
            None
            if provider_graph_v4
            else manifest_artifact_dir
            / f"provider_forward_{manifest_file_token}.ptg2sc"
        ),
        "provider_inverted": (
            None
            if provider_graph_v4
            else manifest_artifact_dir
            / f"provider_inverted_{manifest_file_token}.ptg2sc"
        ),
        "provider_set_component": (
            manifest_artifact_dir
            / f"provider_set_component_{manifest_file_token}.ptg2sc"
            if provider_graph_v4
            else None
        ),
        "provider_component_group": (
            manifest_artifact_dir
            / f"provider_component_group_{manifest_file_token}.ptg2sc"
            if provider_graph_v4
            else None
        ),
        "provider_group_tax_identity": (
            manifest_artifact_dir
            / f"provider_group_tax_identity_{manifest_file_token}.ptg2tax"
            if provider_graph_v4
            else None
        ),
        "provider_group_npi": manifest_artifact_dir
        / f"provider_group_npi_{manifest_file_token}.ptg2sc",
        "provider_npi_group": manifest_artifact_dir
        / f"provider_npi_group_{manifest_file_token}.ptg2sc",
        "provider_npi_scope": (
            manifest_artifact_dir / f"provider_npi_scope_{manifest_file_token}.copy"
            if provider_graph_v4
            else None
        ),
    }

    def discard_file_scratch() -> None:
        """Remove all file-local COPY, run, and sidecar scratch artifacts."""

        _cleanup_manifest_copy_entries(deferred_copy_entries_by_kind)
        for copy_path in (
            manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path,
            manifest_price_set_summary_copy_path,
            manifest_provider_group_member_copy_path,
            manifest_provider_set_metadata_copy_path,
        ):
            _cleanup_manifest_copy_family(copy_path)
        shutil.rmtree(v3_serving_run_directory, ignore_errors=True)
        shutil.rmtree(manifest_artifact_dir, ignore_errors=True)

    def record_ready_manifest_file(
        kind: str,
        copy_row: dict[str, Any],
        *,
        from_recovery: bool = False,
    ) -> None:
        """Record one nonempty deferred COPY file exactly once for publication."""

        if kind not in {
            "price_atom",
            "price_set_atom",
            "price_set_summary",
            "provider_group_member",
            "provider_set_metadata",
        }:
            return
        raw_copy_path = str(copy_row.get("path") or "").strip()
        if not raw_copy_path:
            return
        copy_file = Path(raw_copy_path)
        path_key = _copy_file_key(copy_file)
        seen_paths = deferred_copy_file_paths_by_kind.setdefault(kind, set())
        if from_recovery:
            manifest_copy_file_accounting_by_name["recovery_candidates"] += 1
        if path_key in seen_paths:
            duplicate_counter = (
                "recovery_already_reported_files"
                if from_recovery
                else "scanner_duplicate_files"
            )
            manifest_copy_file_accounting_by_name[duplicate_counter] += 1
            return
        if from_recovery and (not copy_file.exists() or copy_file.stat().st_size <= 0):
            return
        copied_rows = int(copy_row.get("row_count") or 0)
        if copied_rows <= 0:
            file_size = copy_file.stat().st_size if copy_file.exists() else 0
            manifest_copy_file_accounting_by_name["fallback_row_count_files"] += 1
            manifest_copy_file_accounting_by_name[
                "fallback_row_count_bytes"
            ] += file_size
            copied_rows = _ptg2_copy_file_row_count(copy_file)
        _record_deferred_copy_file_once(
            kind,
            copy_file,
            copied_rows,
            metadata=copy_row,
        )
        recorded_counter = (
            "recovered_unreported_files" if from_recovery else "scanner_reported_files"
        )
        manifest_copy_file_accounting_by_name[recorded_counter] += 1

    try:
        raw_source_sha256 = (
            str(source_version.raw_sha256 if source_version is not None else "")
            .strip()
            .lower()
        )
        if len(raw_source_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in raw_source_sha256
        ):
            raise RuntimeError(
                "strict V3 scanner requires the verified raw source SHA-256"
            )
        async for record_kind, record_row in _aiter_compact_serving_records_rust(
            file_path,
            raw_source_sha256=raw_source_sha256,
            snapshot_id=snapshot_id,
            plan_id=str(plan_fields.get("plan_id") or ""),
            coverage_scope_id=coverage_scope_id,
            plan_month_id=str(plan_month_row["plan_month_id"]),
            source_trace_set_hash=source_trace_set_hash,
            manifest_serving_copy_path=None,
            manifest_lean_serving_copy_path=None,
            v3_serving_run_directory=v3_serving_run_directory,
            manifest_provider_forward_sidecar_path=manifest_sidecar_paths_by_kind[
                "provider_forward"
            ],
            manifest_provider_inverted_sidecar_path=manifest_sidecar_paths_by_kind[
                "provider_inverted"
            ],
            manifest_provider_set_component_sidecar_path=manifest_sidecar_paths_by_kind[
                "provider_set_component"
            ],
            manifest_provider_component_group_sidecar_path=manifest_sidecar_paths_by_kind[
                "provider_component_group"
            ],
            manifest_provider_group_tax_identity_sidecar_path=(
                manifest_sidecar_paths_by_kind["provider_group_tax_identity"]
            ),
            manifest_provider_npi_sidecar_path=None,
            manifest_price_forward_sidecar_path=None,
            manifest_price_atom_copy_path=manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path=manifest_price_set_atom_copy_path,
            manifest_price_set_summary_copy_path=manifest_price_set_summary_copy_path,
            manifest_provider_group_member_copy_path=manifest_provider_group_member_copy_path,
            manifest_code_count_copy_path=None,
            manifest_provider_set_dictionary_copy_path=manifest_provider_set_metadata_copy_path,
            source_network_names=source_network_name_values,
            manifest_only=True,
            progress_observer=progress_observer,
        ):
            if record_kind == "dedupe_summary":
                rust_dedupe_summary_by_field = dict(record_row or {})
                continue
            if record_kind == "scanner_config":
                rust_scanner_config_by_name = dict(record_row or {})
                continue
            if record_kind == "scanner_summary":
                rust_scanner_summary_by_name = dict(record_row or {})
                _record_v3_scanner_summary(
                    rust_scanner_summary_by_name,
                    deferred_copy_entries_by_kind,
                    manifest_copy_row_counter_by_name,
                )
                continue
            if record_kind == "source_audit_witness_file":
                witness_entry_by_field = dict(record_row or {})
                if witness_entry_by_field.get("raw_source_sha256") != raw_source_sha256:
                    raise RuntimeError(
                        "strict V3 source witness digest does not match its input"
                    )
                witness_path = Path(str(witness_entry_by_field.get("path") or ""))
                if not witness_path.is_file():
                    raise RuntimeError("strict V3 source witness file is missing")
                deferred_copy_entries_by_kind["source_audit_witness"].append(
                    witness_entry_by_field
                )
                continue
            if record_kind == "manifest_provider_group_tax_identity_sidecar_file":
                if not provider_graph_v4:
                    raise RuntimeError(
                        "strict V3 scanner emitted an unexpected provider-group "
                        "tax identity artifact"
                    )
                if provider_group_tax_identity_artifact_by_field is not None:
                    raise RuntimeError(
                        "PTG V4 scanner emitted duplicate provider-group tax "
                        "identity evidence"
                    )
                provider_group_tax_identity_artifact_by_field = dict(record_row or {})
                continue
            rust_records += 1
            if record_kind == "manifest_price_atom_copy_file":
                record_ready_manifest_file("price_atom", record_row)
            if record_kind == "manifest_price_set_atom_copy_file":
                record_ready_manifest_file("price_set_atom", record_row)
            if record_kind == "manifest_price_set_summary_copy_file":
                record_ready_manifest_file("price_set_summary", record_row)
            if record_kind == "manifest_provider_group_member_copy_file":
                record_ready_manifest_file("provider_group_member", record_row)
            if record_kind == "manifest_provider_set_dictionary_copy_file":
                record_ready_manifest_file("provider_set_metadata", record_row)
            if record_kind in {"procedure", "serving_rate_compact"} and record_row.get(
                "procedure_hash"
            ):
                procedure_hashes.add(str(record_row.get("procedure_hash")))
        for copy_path, kind in (
            (manifest_price_atom_copy_path, "price_atom"),
            (manifest_price_set_atom_copy_path, "price_set_atom"),
            (manifest_price_set_summary_copy_path, "price_set_summary"),
            (manifest_provider_group_member_copy_path, "provider_group_member"),
            (manifest_provider_set_metadata_copy_path, "provider_set_metadata"),
        ):
            for candidate_copy_path in _manifest_copy_candidates(copy_path):
                record_ready_manifest_file(
                    kind,
                    {"path": str(candidate_copy_path), "row_count": 0},
                    from_recovery=True,
                )
        if provider_graph_v4 and provider_group_tax_identity_artifact_by_field is None:
            raise RuntimeError(
                "PTG V4 scanner omitted provider-group tax identity evidence"
            )
        is_scan_complete = True
    finally:
        manifest_copy_paths = (
            manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path,
            manifest_price_set_summary_copy_path,
            manifest_provider_group_member_copy_path,
            manifest_provider_set_metadata_copy_path,
        )
        for copy_path in manifest_copy_paths:
            try:
                if copy_path.exists() and copy_path.stat().st_size == 0:
                    copy_path.unlink(missing_ok=True)
            except Exception:
                logger.debug(
                    "Failed to remove empty PTG2 manifest copy file %s",
                    copy_path,
                    exc_info=True,
                )
            _cleanup_empty_manifest_copy_siblings(copy_path)
        if not is_scan_complete:
            for copy_path in manifest_copy_paths:
                _cleanup_manifest_copy_family(copy_path)
            discard_file_scratch()

    membership_graph_metrics_map: dict[str, Any] = {}
    provider_npi_scope_copy_path = (
        manifest_sidecar_paths_by_kind["provider_npi_scope"]
        if provider_graph_v4
        else _new_copy_path("ptg2_manifest_provider_npi_scope_")
    )
    if provider_npi_scope_copy_path is None:
        raise RuntimeError("PTG2 provider NPI scope path is unavailable")
    provider_npi_scope_copy_path.unlink(missing_ok=True)
    provider_group_member_paths = [
        Path(copy_metadata["path"])
        for copy_metadata in deferred_copy_entries_by_kind["provider_group_member"]
        if copy_metadata.get("path")
    ]
    try:
        membership_graph_metrics_map = await _build_ptg2_provider_membership_sidecars(
            provider_group_npi_path=manifest_sidecar_paths_by_kind[
                "provider_group_npi"
            ],
            provider_npi_group_path=manifest_sidecar_paths_by_kind[
                "provider_npi_group"
            ],
            provider_npi_scope_copy_path=provider_npi_scope_copy_path,
            input_paths=provider_group_member_paths,
        )
    except BaseException:
        provider_npi_scope_copy_path.unlink(missing_ok=True)
        discard_file_scratch()
        raise
    if not provider_graph_v4:
        provider_npi_scope_copy_path.unlink(missing_ok=True)
    _cleanup_manifest_copy_entries(
        {
            "provider_group_member": deferred_copy_entries_by_kind[
                "provider_group_member"
            ]
        }
    )
    deferred_copy_entries_by_kind["provider_group_member"] = []

    try:
        await flush_error_log(import_log_cls)
    except BaseException:
        discard_file_scratch()
        raise
    manifest_artifacts = _collect_ptg2_manifest_sidecar_artifacts(
        manifest_sidecar_paths_by_kind,
        provider_group_tax_identity_artifact=(
            provider_group_tax_identity_artifact_by_field
        ),
        membership_graph_metrics=membership_graph_metrics_map,
    )
    import_summary_by_field = {
        "provider_refs": 0,
        "in_network_items": len(procedure_hashes),
        "serving_rates": manifest_copy_row_counter_by_name["serving"],
        "serving_only": True,
        "serving_workers": 0,
        "worker_chunk_items": 0,
        "rust_manifest_serving": True,
        "rust_records": rust_records,
        "manifest": {
            "serving_rows": manifest_copy_row_counter_by_name["serving"],
            "source_trace_hash": source_trace_hash,
            "source_trace_set_hash": source_trace_set_hash,
            "network_names": source_network_name_values,
            "sidecars": manifest_artifacts,
            "sidecar_paths": {
                name: str(path)
                for name, path in manifest_sidecar_paths_by_kind.items()
                if path is not None
            },
            "copy_files": deferred_copy_entries_by_kind,
            "copy_file_accounting": manifest_copy_file_accounting_by_name,
            "precopy_merge_deferred": True,
            "membership_graph": membership_graph_metrics_map,
        },
    }
    if rust_dedupe_summary_by_field:
        import_summary_by_field["dedupe"] = rust_dedupe_summary_by_field
    if rust_scanner_config_by_name or rust_scanner_summary_by_name:
        import_summary_by_field["scanner"] = {
            "config": rust_scanner_config_by_name,
            "summary": rust_scanner_summary_by_name,
        }
    _emit_screen_line(f"PTG2 serving-only import summary: {import_summary_by_field}")
    logger.info("PTG2 serving-only import summary: %s", import_summary_by_field)
    return import_summary_by_field


_parse_in_network_file_strict_v3 = _parse_strict_v3_file


@dataclass
class _StageTimer:
    durations_by_stage: dict[str, float]
    started_monotonic: float

    def mark(self, stage_name: str) -> None:
        """Record elapsed time and advance the stage boundary."""
        now_monotonic = _ptg2_monotonic()
        self.durations_by_stage[stage_name] = now_monotonic - self.started_monotonic
        self.started_monotonic = now_monotonic


@dataclass
class _PendingStrictV3State:
    copy_entries_by_kind: dict[str, list[dict[str, Any]]]
    graph_artifacts_map: dict[str, Any]


def _pending_sidecar_entries(
    file_summary: Mapping[str, Any],
) -> list[dict[str, Any]] | None:
    summary_payload = file_summary.get("summary")
    manifest_payload = (
        summary_payload.get("manifest")
        if isinstance(summary_payload, Mapping)
        else None
    )
    if not isinstance(manifest_payload, Mapping):
        return None
    raw_sidecars = manifest_payload.get("sidecars") or ()
    if isinstance(raw_sidecars, Mapping):
        sidecar_entries = [
            dict(sidecar_entry)
            for sidecar_entry in raw_sidecars.values()
            if isinstance(sidecar_entry, Mapping)
        ]
    elif isinstance(raw_sidecars, Sequence) and not isinstance(
        raw_sidecars,
        (str, bytes, bytearray),
    ):
        sidecar_entries = [
            dict(sidecar_entry)
            for sidecar_entry in raw_sidecars
            if isinstance(sidecar_entry, Mapping)
        ]
    else:
        sidecar_entries = []
    raw_sidecar_paths = manifest_payload.get("sidecar_paths")
    if not sidecar_entries and isinstance(raw_sidecar_paths, Mapping):
        return [
            {"name": str(name), "path": str(path)}
            for name, path in raw_sidecar_paths.items()
            if str(path or "").strip()
        ]
    return sidecar_entries


def _merge_pending_sidecars(
    pending_state: _PendingStrictV3State,
    sidecar_entries: Sequence[Mapping[str, Any]],
) -> None:
    pending_sidecars = pending_state.graph_artifacts_map.setdefault(
        "sidecars",
        [],
    )
    if not isinstance(pending_sidecars, list):
        pending_sidecars = []
        pending_state.graph_artifacts_map["sidecars"] = pending_sidecars
    seen_sidecar_paths = {
        str(sidecar_entry.get("path") or "").strip()
        for sidecar_entry in pending_sidecars
        if isinstance(sidecar_entry, Mapping)
    }
    for sidecar_entry in sidecar_entries:
        sidecar_path = str(sidecar_entry.get("path") or "").strip()
        if not sidecar_path or sidecar_path in seen_sidecar_paths:
            continue
        seen_sidecar_paths.add(sidecar_path)
        pending_sidecars.append(dict(sidecar_entry))


def _register_strict_v3_pending_file(
    pending_state: _PendingStrictV3State,
    file_summary: Mapping[str, Any],
) -> None:
    """Transfer one completed file's scratch ownership to import cleanup."""

    incoming_entries = _pending_strict_v3_copy_entries([dict(file_summary)])
    for kind, entries in incoming_entries.items():
        pending_entries = pending_state.copy_entries_by_kind.setdefault(kind, [])
        seen_paths = {
            str(entry.get("path") or "").strip()
            for entry in pending_entries
            if isinstance(entry, Mapping)
        }
        for entry in entries:
            path = str(entry.get("path") or "").strip()
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            pending_entries.append(dict(entry))

    sidecar_entries = _pending_sidecar_entries(file_summary)
    if sidecar_entries is None:
        return
    _merge_pending_sidecars(pending_state, sidecar_entries)


def _claim_strict_v3_file_scratch(
    pending_state: _PendingStrictV3State,
    file_result: PTG2FileProcessResult | None,
) -> None:
    """Claim completed-file scratch before its task result can be discarded."""

    if file_result is None or not file_result.success or file_result.skipped:
        return
    _register_strict_v3_pending_file(pending_state, asdict(file_result))


def _claim_strict_v3_file_result(
    pending_state: _PendingStrictV3State,
    file_result: PTG2FileProcessResult,
    physical_identity: SharedPhysicalArtifactIdentity,
    logical_artifact_metadata: Mapping[str, Any],
) -> PTG2FileProcessResult:
    """Keep completed scratch owned even when source-contract annotation fails."""

    claimed_result = file_result
    try:
        claimed_result = _annotate_v3_file_result_source_identity(
            file_result,
            physical_identity,
            logical_artifact_metadata,
        )
        return claimed_result
    finally:
        _claim_strict_v3_file_scratch(pending_state, claimed_result)


def _toc_file_url_match_tokens(file_url_contains: list[str] | None) -> list[str]:
    """Normalize targeted source-file URL filters for early TOC entry checks."""
    return [
        str(value or "").strip().lower()
        for value in (file_url_contains or [])
        if str(value or "").strip()
    ]


def _is_requested_toc_body_file_url(
    location: str, file_url_match_tokens: list[str]
) -> bool:
    """Return whether a TOC body-file URL satisfies the requested file filters."""
    if not file_url_match_tokens:
        return True
    normalized_location = str(location or "").lower()
    return any(token in normalized_location for token in file_url_match_tokens)


def _include_toc_job_with_limit(
    jobs: list[dict[str, Any]],
    selected_job_identities: set[tuple[str, str]],
    job: dict[str, Any],
    max_files: int | None,
) -> bool:
    """Select one physical file while retaining every matching plan scope."""

    identity = _ptg_job_identity(job)
    if (
        identity not in selected_job_identities
        and max_files is not None
        and len(selected_job_identities) >= max_files
    ):
        return False
    selected_job_identities.add(identity)
    jobs.append(job)
    return True


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
    artifact_stage_observer: PTG2ArtifactStageObserver | None = None,
) -> list[dict[str, Any]]:
    """Download and filter one table of contents, persist files, and return jobs."""
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    jobs: list[dict[str, Any]] = []
    selected_job_identities: set[tuple[str, str]] = set()
    file_rows: list[dict[str, Any]] = []
    allowed_job_candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen_files: set[int] = set()
    body_file_limit = max_files
    if test_mode:
        body_file_limit = min(
            TEST_TOC_JOBS,
            body_file_limit if body_file_limit is not None else TEST_TOC_JOBS,
        )

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                toc_url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
                **(
                    {"artifact_stage_observer": artifact_stage_observer}
                    if artifact_stage_observer is not None
                    else {}
                ),
            )
        except (
            PTG2ArtifactStageFreshnessError,
            PTG2FullRebuildFreshnessError,
        ):
            raise
        except Exception as exc:
            logger.warning(
                "Failed to download table-of-contents from %s: %s", toc_url, exc
            )
            if raise_on_error:
                raise RuntimeError(
                    f"Failed to download table-of-contents from {toc_url}: {exc}"
                ) from exc
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
    parsed_catalog_entries: list[PTG2SourceCatalogEntry] = []
    if not targeted_file_import or not toc_content.get("reporting_structure"):
        parsed_catalog_entries = parse_toc_catalog_entries(
            toc_content,
            toc_url,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )

    if import_run_id and not targeted_file_import:
        catalog_rows = []
        for entry in parsed_catalog_entries:
            if entry.domain not in {
                PTG2_DOMAIN_IN_NETWORK,
                PTG2_DOMAIN_ALLOWED_AMOUNT,
            }:
                continue
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
                    "plan_sponsor_name": first_plan.get("plan_sponsor_name")
                    or first_plan.get("plan_sponser_name"),
                    "payload": _canonicalize_for_json(entry),
                    "created_at": _utcnow(),
                }
            )
        if catalog_rows:
            await _push_ptg2_objects(catalog_rows, PTG2SourceCatalog, rewrite=True)

    toc_metadata_by_field = {
        "reporting_entity_name": toc_content.get("reporting_entity_name"),
        "reporting_entity_type": toc_content.get("reporting_entity_type"),
        "last_updated_on": toc_content.get("last_updated_on"),
        "version": toc_content.get("version"),
    }
    file_rows.append(
        _build_file_row(
            toc_url,
            "table-of-contents",
            toc_metadata_by_field,
            None,
            toc_content.get("description"),
            None,
        )
    )

    if not toc_content.get("reporting_structure"):
        for catalog_entry in parsed_catalog_entries:
            if catalog_entry.domain == PTG2_DOMAIN_IN_NETWORK:
                job_type = "in_network"
                file_type = "in-network"
            elif catalog_entry.domain == PTG2_DOMAIN_ALLOWED_AMOUNT:
                job_type = "allowed_amounts"
                file_type = "allowed-amounts"
            else:
                continue
            location = normalize_tic_source_url(catalog_entry.original_url)
            if not _is_requested_toc_body_file_url(location, file_url_match_tokens):
                continue
            file_metadata_by_field = {
                "reporting_entity_name": catalog_entry.reporting_entity_name,
                "reporting_entity_type": catalog_entry.reporting_entity_type,
            }
            plans = list(catalog_entry.plan_info or ())
            file_row = _build_file_row(
                location,
                file_type,
                file_metadata_by_field,
                plans,
                catalog_entry.description,
                catalog_entry.from_index_url or toc_url,
            )
            job_by_field = {
                "type": job_type,
                "url": location,
                "description": catalog_entry.description,
                "plan_info": plans,
                "from_index_url": catalog_entry.from_index_url or toc_url,
                "meta": file_metadata_by_field,
            }
            if job_type == "allowed_amounts":
                allowed_job_candidates.append((job_by_field, file_row))
                continue
            if not _include_toc_job_with_limit(
                jobs,
                selected_job_identities,
                job_by_field,
                body_file_limit,
            ):
                continue
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])

    for structure in toc_content.get("reporting_structure", []):
        plans = _filter_reporting_plans(
            [
                _normalize_plan_payload(plan)
                for plan in (structure.get("reporting_plans") or [])
            ],
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        in_network_files = [
            file_entry
            for file_entry in _as_list(structure.get("in_network_files"))
            if isinstance(file_entry, dict)
        ]
        for entry in in_network_files:
            location = entry.get("location")
            if not _is_toc_body_file_location(location):
                continue
            location = normalize_tic_source_url(location)
            if not _is_requested_toc_body_file_url(location, file_url_match_tokens):
                continue
            file_metadata_by_field = dict(toc_metadata_by_field)
            file_row = _build_file_row(
                location,
                "in-network",
                file_metadata_by_field,
                plans,
                entry.get("description"),
                toc_url,
            )
            job_by_field = {
                "type": "in_network",
                "url": location,
                "description": entry.get("description"),
                "plan_info": plans,
                "from_index_url": toc_url,
                "meta": file_metadata_by_field,
            }
            if not _include_toc_job_with_limit(
                jobs,
                selected_job_identities,
                job_by_field,
                body_file_limit,
            ):
                continue
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])

        allowed_amount_files = _as_list(
            structure.get("allowed_amount_file")
        ) + _as_list(structure.get("allowed_amount_files"))
        for entry in allowed_amount_files:
            if not isinstance(entry, dict):
                continue
            location = entry.get("location")
            if not _is_toc_body_file_location(location):
                continue
            location = normalize_tic_source_url(location)
            if not _is_requested_toc_body_file_url(
                location,
                file_url_match_tokens,
            ):
                continue
            file_metadata_by_field = dict(toc_metadata_by_field)
            file_row = _build_file_row(
                location,
                "allowed-amounts",
                file_metadata_by_field,
                plans,
                entry.get("description"),
                toc_url,
            )
            allowed_job_candidates.append(
                (
                    {
                        "type": "allowed_amounts",
                        "url": location,
                        "description": entry.get("description"),
                        "plan_info": plans,
                        "from_index_url": toc_url,
                        "meta": file_metadata_by_field,
                    },
                    file_row,
                )
            )

    for allowed_job, file_row in allowed_job_candidates:
        if not _include_toc_job_with_limit(
            jobs,
            selected_job_identities,
            allowed_job,
            body_file_limit,
        ):
            continue
        if file_row["file_id"] not in seen_files:
            file_rows.append(file_row)
            seen_files.add(file_row["file_id"])

    if file_rows:
        await push_objects(file_rows, file_cls, rewrite=True)
    await flush_error_log(import_log_cls)
    return jobs


def _is_tic_toc_json_text(text: str) -> bool:
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


def _json_string_scan_state(
    char: str,
    *,
    is_in_string: bool,
    is_escaped: bool,
) -> tuple[bool, bool, bool]:
    if not is_in_string:
        return char == '"', False, char == '"'
    if is_escaped:
        return True, False, True
    if char == "\\":
        return True, True, True
    if char == '"':
        return False, False, True
    return True, False, True


def _repair_missing_array_object_commas(text: str) -> str:
    repaired_chars: list[str] = []
    is_in_string = False
    is_escaped = False
    length = len(text)
    for idx, char in enumerate(text):
        repaired_chars.append(char)
        is_in_string, is_escaped, should_continue = _json_string_scan_state(
            char,
            is_in_string=is_in_string,
            is_escaped=is_escaped,
        )
        if should_continue:
            continue
        if char != "}":
            continue
        lookahead = idx + 1
        while lookahead < length and text[lookahead].isspace():
            lookahead += 1
        if lookahead < length and text[lookahead] == "{":
            repaired_chars.append(",")
    return "".join(repaired_chars)


def _load_table_of_contents_artifact(path: str | Path) -> dict[str, Any]:
    try:
        toc = load_json_artifact(path)
    except json.JSONDecodeError:
        with open_json_artifact_stream(path) as fp:
            raw = fp.read()
        text = raw.decode("utf-8", errors="replace")
        if not _is_tic_toc_json_text(text):
            raise
        toc = json.loads(_repair_missing_array_object_commas(text))
    if not isinstance(toc, dict):
        raise ValueError("expected table-of-contents JSON object")
    return toc


async def _record_in_network_file_provenance(
    job: dict[str, Any],
    classes: Mapping[str, type],
    *,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
    import_run_id: str | None,
) -> dict[str, Any]:
    """Persist logical file/source metadata independently from scanner dedupe."""

    provided_meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
    meta = provided_meta or await _extract_metadata_fields(
        logical_artifact.logical_path
    )
    plan_info = job.get("plan_info") if isinstance(job.get("plan_info"), list) else None
    file_row = _build_file_row(
        str(job.get("url") or raw_artifact.original_url),
        "in-network",
        meta,
        plan_info,
        job.get("description"),
        job.get("from_index_url"),
    )
    await _push_ptg2_objects([file_row], classes["PTGFile"], rewrite=True)
    source_version = await _record_source_version(
        source_type="in-network",
        domain=PTG2_DOMAIN_IN_NETWORK,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
        import_run_id=import_run_id,
    )
    source_trace_row, source_trace_set_row = _ptg2_source_trace_rows(
        source_version,
        str(job.get("url") or raw_artifact.original_url),
    )
    await _push_ptg2_objects([source_trace_row], PTG2SourceTrace, rewrite=True)
    await _push_ptg2_objects(
        [source_trace_set_row],
        PTG2SourceTraceSet,
        rewrite=True,
    )
    return {
        "file_row": file_row,
        "meta": meta,
        "source_version": source_version,
        "source_trace_hash": source_trace_row["source_trace_hash"],
        "source_trace_set_hash": source_trace_set_row["source_trace_set_hash"],
        "network_names": _normalize_source_network_names(
            job.get("source_network_names") or []
        ),
    }


@dataclass
class _InNetworkFileContext:
    job: dict[str, Any]
    classes: dict[str, type]
    test_mode: bool
    reuse_raw_artifacts: bool = True
    max_bytes: int | None = None
    max_items: int | None = None
    import_run_id: str | None = None
    keep_partial_artifacts: bool | None = None
    snapshot_id: str | None = None
    coverage_scope_id: str | None = None
    import_month: datetime.date | None = None
    ptg2_manifest_stage_table: str | None = None
    source_network_names: list[str] | str | None = None
    raw_artifact: PTG2RawArtifact | None = None
    logical_artifact: PTG2LogicalArtifact | None = None
    recorded_provenance: Mapping[str, Any] | None = None
    progress_observer: Callable[[dict[str, Any]], None] | None = None


@dataclass(frozen=True)
class _InNetworkParseResult:
    url: str
    file_id: str
    source_version: Any
    parse_summary: Mapping[str, Any] | None


async def _in_network_artifacts(
    context: _InNetworkFileContext,
    temporary_directory: str,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    if context.raw_artifact is not None and context.logical_artifact is not None:
        return context.raw_artifact, context.logical_artifact
    return await materialize_json_source(
        str(context.job["url"]),
        temporary_directory,
        reuse_raw_artifacts=context.reuse_raw_artifacts,
        max_bytes=context.max_bytes,
        materialize_logical=False,
        keep_partial_artifacts=context.keep_partial_artifacts,
    )


async def _in_network_provenance(
    context: _InNetworkFileContext,
    *,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
) -> dict[str, Any]:
    provenance_by_field = dict(context.recorded_provenance or {})
    if provenance_by_field:
        return provenance_by_field
    return await _record_in_network_file_provenance(
        context.job,
        context.classes,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
        import_run_id=context.import_run_id,
    )


async def _parse_in_network_artifact(
    context: _InNetworkFileContext,
    *,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
) -> _InNetworkParseResult:
    provenance_by_field = await _in_network_provenance(
        context,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
    )
    source_metadata_by_field = dict(provenance_by_field["meta"])
    file_record_by_field = dict(provenance_by_field["file_row"])
    source_version = provenance_by_field["source_version"]
    url = str(context.job["url"])
    source_network_names = _normalize_source_network_names(
        context.source_network_names or context.job.get("source_network_names")
    )
    parse_summary = await _parse_in_network_file_strict_v3(
        logical_artifact.logical_path,
        file_record_by_field["file_id"],
        source_metadata_by_field,
        context.job.get("plan_info"),
        context.test_mode,
        context.classes["ImportLog"],
        url,
        source_version,
        context.snapshot_id or "ptg2:unknown",
        str(context.coverage_scope_id),
        context.import_month or normalize_import_month(None),
        max_items=context.max_items,
        ptg2_manifest_stage_table=context.ptg2_manifest_stage_table,
        source_network_names=source_network_names,
        progress_observer=context.progress_observer,
    )
    return _InNetworkParseResult(
        url=url,
        file_id=file_record_by_field["file_id"],
        source_version=source_version,
        parse_summary=parse_summary,
    )


def _in_network_file_result(
    context: _InNetworkFileContext,
    parsed: _InNetworkParseResult,
) -> PTG2FileProcessResult:
    if (
        not context.test_mode
        and int((parsed.parse_summary or {}).get("serving_rates") or 0) <= 0
    ):
        summary_by_field = dict(parsed.parse_summary or {})
        summary_by_field["skipped_reason"] = "parsed zero serving rates"
        summary_by_field.update(_source_version_summary(parsed.source_version))
        return PTG2FileProcessResult(
            "in_network",
            parsed.url,
            True,
            file_id=parsed.file_id,
            summary=summary_by_field,
            skipped=True,
        )
    summary_by_field = dict(parsed.parse_summary or {})
    summary_by_field.update(_source_version_summary(parsed.source_version))
    return PTG2FileProcessResult(
        "in_network",
        parsed.url,
        True,
        file_id=parsed.file_id,
        summary=summary_by_field,
    )


async def _process_in_network_file(
    context: _InNetworkFileContext,
) -> PTG2FileProcessResult:
    """Scan one in-network job into strict V3 staging and return its result."""

    url = str(context.job["url"])
    if not context.coverage_scope_id or not re.fullmatch(
        r"[0-9a-f]{64}",
        context.coverage_scope_id,
    ):
        raise ValueError(
            "strict V3 file processing requires a 32-byte coverage scope id"
        )
    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await _in_network_artifacts(
                context,
                tmpdir,
            )
        except Exception as exc:
            logger.warning(
                "Failed to download in-network file from %s: %s",
                url,
                exc,
            )
            return PTG2FileProcessResult(
                "in_network",
                url,
                False,
                error=str(exc),
            )
        parsed = await _parse_in_network_artifact(
            context,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
        )
    return _in_network_file_result(context, parsed)


@dataclass
class _CompletedImportPersistence:
    import_run_id: str
    import_month: datetime.date
    started_at: datetime.datetime
    options: Mapping[str, Any]
    report_payload: dict[str, Any]
    timing_payload: dict[str, Any]
    import_started_monotonic: float
    post_publish_started_monotonic: float
    post_publish_stage_timer: _StageTimer
    snapshot_id: str | None = None
    manifest_stage_table: str | None = None


def _completed_import_run_row(
    state: _CompletedImportPersistence,
    *,
    finished_at: datetime.datetime,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "import_run_id": state.import_run_id,
        "import_month": state.import_month,
        "status": PTG2_STATUS_VALIDATED,
        "started_at": state.started_at,
        "finished_at": finished_at,
        "heartbeat_at": finished_at,
        "options": dict(state.options),
        "report": dict(report),
        "error": None,
    }


def _finalize_completion_report(
    state: _CompletedImportPersistence,
) -> None:
    completed_monotonic = _ptg2_monotonic()
    for key, stage_seconds in state.post_publish_stage_timer.durations_by_stage.items():
        state.timing_payload[f"post_publish_{key}_seconds"] = stage_seconds
    state.timing_payload["post_publish_seconds"] = (
        completed_monotonic - state.post_publish_started_monotonic
    )
    state.timing_payload["total_seconds"] = (
        completed_monotonic - state.import_started_monotonic
    )
    state.report_payload["timings"] = state.timing_payload
    state.report_payload["timing_contract"] = {
        "version": 2,
        "total_boundary": "after_required_run_state_persistence",
        "completion_metrics_write_excluded": True,
    }


async def _persist_completed_ptg2_import_run(
    state: _CompletedImportPersistence,
) -> datetime.datetime:
    """Atomically persist completion and release owned manifest stages."""

    if bool(state.snapshot_id) != bool(state.manifest_stage_table):
        raise ValueError(
            "completion stage release requires both snapshot and stage identifiers"
        )

    provisional_finished_at = _utcnow()
    provisional_report_by_field = {
        **state.report_payload,
        "timings": dict(state.timing_payload),
        "timing_contract": {
            "version": 2,
            "completion_metrics_pending": True,
        },
    }
    async with db.transaction():
        await _push_ptg2_objects(
            [
                _completed_import_run_row(
                    state,
                    finished_at=provisional_finished_at,
                    report=provisional_report_by_field,
                )
            ],
            PTG2ImportRun,
            rewrite=True,
        )
        state.post_publish_stage_timer.mark("run_state_persistence")
        if state.manifest_stage_table is not None:
            assert state.snapshot_id is not None
            await _drop_ptg2_snapshot_table_names(
                _ptg2_manifest_stage_table_names(state.manifest_stage_table),
                snapshot_id=state.snapshot_id,
                internal_run_id=state.import_run_id,
            )
            state.post_publish_stage_timer.mark("manifest_stage_release")
        _finalize_completion_report(state)
        completed_at = _utcnow()
        await _push_ptg2_objects(
            [
                _completed_import_run_row(
                    state,
                    finished_at=completed_at,
                    report=state.report_payload,
                )
            ],
            PTG2ImportRun,
            rewrite=True,
        )
    return completed_at


def _terminal_retry_update_statement(schema: str) -> Any:
    return db.text(
        f"""
        UPDATE {schema}.ptg2_import_run AS internal_run
           SET status = CAST(:terminal_run_status AS varchar(32)),
               finished_at = CASE
                   WHEN internal_run.status = CAST(
                       :terminal_run_status AS varchar(32)
                   )
                    AND internal_run.finished_at IS NOT NULL
                   THEN internal_run.finished_at
                   ELSE statement_timestamp()
               END,
               heartbeat_at = CASE
                   WHEN internal_run.status = CAST(
                       :terminal_run_status AS varchar(32)
                   )
                   THEN COALESCE(
                       internal_run.heartbeat_at,
                       internal_run.finished_at,
                       statement_timestamp()
                   )
                   ELSE statement_timestamp()
               END,
               report = CASE
                   WHEN internal_run.status = CAST(
                       :terminal_run_status AS varchar(32)
                   )
                    AND COALESCE(
                       internal_run.report::jsonb,
                       '{{}}'::jsonb
                   ) <> '{{}}'::jsonb
                   THEN internal_run.report
                   ELSE snapshot.manifest
               END,
               error = NULL
          FROM {schema}.ptg2_snapshot AS snapshot
         WHERE internal_run.import_run_id = :internal_run_id
           AND snapshot.snapshot_id = :snapshot_id
           AND snapshot.import_run_id = internal_run.import_run_id
           AND snapshot.status IN (:validated_status, :published_status)
        RETURNING internal_run.import_run_id
        """
    )


async def _registered_terminal_stage_names(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    internal_run_id: str,
) -> list[str]:
    stage_result = await session.execute(
        db.text(
            f"""
            SELECT table_name
              FROM {schema}.ptg2_v4_attempt_stage
             WHERE snapshot_id = :snapshot_id
               AND internal_run_id = :internal_run_id
             ORDER BY table_name
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "internal_run_id": internal_run_id,
        },
    )
    return [str(stage_record[0]) for stage_record in stage_result.all()]


async def _finalize_resumed_terminal_attempt(
    snapshot_attributes: Mapping[str, Any],
    *,
    internal_run_id: str,
) -> None:
    """Finish an exact terminal retry and remove any retained V4 stages."""

    snapshot_id = str(snapshot_attributes.get("snapshot_id") or "")
    snapshot_run_id = str(snapshot_attributes.get("import_run_id") or "")
    if not snapshot_id or snapshot_run_id != internal_run_id:
        raise RuntimeError("terminal snapshot retry changed its attempt pair")
    schema_name = resolve_ptg2_schema()
    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        await lock_writable_snapshot(
            session,
            db,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        terminal_result = await session.execute(
            _terminal_retry_update_statement(schema),
            {
                "terminal_run_status": PTG2_STATUS_VALIDATED,
                "internal_run_id": internal_run_id,
                "snapshot_id": snapshot_id,
                "validated_status": PTG2_STATUS_VALIDATED,
                "published_status": PTG2_STATUS_PUBLISHED,
            },
        )
        if terminal_result.first() is None:
            raise RuntimeError("terminal snapshot retry is not finalizable")
        registered_stage_names = await _registered_terminal_stage_names(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        if registered_stage_names:
            await _drop_ptg2_snapshot_table_names(
                registered_stage_names,
                snapshot_id=snapshot_id,
                internal_run_id=internal_run_id,
            )


async def _heartbeat_ptg2_import_run(import_run_id: str) -> None:
    """Keep the internal PTG run lease current while a long import is active."""

    interval = max(
        float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15")),
        1.0,
    )
    schema_name = resolve_ptg2_schema()
    schema = _quote_ident(schema_name)
    while True:
        await asyncio.sleep(interval)
        try:
            async with db.transaction() as session:
                await lock_writable_snapshot(
                    session,
                    db,
                    schema_name=schema_name,
                    snapshot_id="",
                    internal_run_id=import_run_id,
                )
                await session.execute(
                    db.text(
                        f"""
                        UPDATE {schema}.ptg2_import_run
                           SET heartbeat_at = timezone(
                               'UTC',
                               statement_timestamp()
                           )
                         WHERE import_run_id = :import_run_id
                           AND status IN ('pending', 'running', 'building')
                        """
                    ),
                    {"import_run_id": import_run_id},
                )
        except Exception as exc:
            if is_stale_metadata_fence_error(exc):
                raise_stale_metadata_fence(exc)
            logger.warning(
                "Failed to persist PTG2 import heartbeat for %s",
                import_run_id,
                exc_info=True,
            )


async def _stop_ptg2_import_heartbeat(task: asyncio.Task[Any] | None) -> None:
    if task is None:
        return
    if task.done():
        if task.cancelled():
            return
        task.result()
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


@dataclass
class _FailedImportPersistence:
    import_run_id: str
    snapshot_id: str
    import_month: datetime.date
    started_at: datetime.datetime
    error: BaseException | str
    report: dict[str, Any] | None = None
    options: dict[str, Any] | None = None
    manifest_stage_table: str | None = None
    should_preserve_published_snapshot: bool = False
    import_started_monotonic: float | None = None
    failure_handling_started_monotonic: float | None = None


def _failed_snapshot_row(
    state: _FailedImportPersistence,
    report_by_field: Mapping[str, Any],
    error_text: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": state.snapshot_id,
        "import_run_id": state.import_run_id,
        "import_month": state.import_month,
        "status": PTG2_STATUS_FAILED,
        "created_at": state.started_at,
        "validated_at": None,
        "published_at": None,
        "previous_snapshot_id": None,
        "manifest": {
            **report_by_field,
            "error": error_text,
        },
    }


def _failed_import_run_row(
    state: _FailedImportPersistence,
    *,
    finished_at: datetime.datetime,
    report: Mapping[str, Any],
    error_text: str,
) -> dict[str, Any]:
    return {
        "import_run_id": state.import_run_id,
        "import_month": state.import_month,
        "status": PTG2_STATUS_FAILED,
        "started_at": state.started_at,
        "finished_at": finished_at,
        "heartbeat_at": finished_at,
        "options": dict(state.options or {}),
        "report": dict(report),
        "error": error_text,
    }


def _finalize_failure_report(
    state: _FailedImportPersistence,
    report_by_field: dict[str, Any],
    timing_by_metric: dict[str, Any],
    *,
    persistence_started_monotonic: float,
) -> None:
    persisted_monotonic = _ptg2_monotonic()
    timing_by_metric["failure_state_persistence_seconds"] = (
        persisted_monotonic - persistence_started_monotonic
    )
    if state.failure_handling_started_monotonic is not None:
        timing_by_metric["failure_handling_seconds"] = (
            persisted_monotonic - state.failure_handling_started_monotonic
        )
    assert state.import_started_monotonic is not None
    timing_by_metric["total_seconds"] = (
        persisted_monotonic - state.import_started_monotonic
    )
    report_by_field["timings"] = timing_by_metric
    report_by_field["timing_contract"] = {
        "version": 2,
        "total_boundary": "after_required_failure_state_persistence",
        "completion_metrics_write_excluded": True,
    }


async def _persist_provisional_failure(
    state: _FailedImportPersistence,
    report_by_field: dict[str, Any],
    *,
    error_text: str,
) -> None:
    provisional_finished_at = _utcnow()
    if not state.should_preserve_published_snapshot:
        await _push_ptg2_objects(
            [_failed_snapshot_row(state, report_by_field, error_text)],
            PTG2Snapshot,
            rewrite=True,
        )
    provisional_report_by_field = {
        **report_by_field,
        "timing_contract": {
            "version": 2,
            "completion_metrics_pending": True,
        },
    }
    await _push_ptg2_objects(
        [
            _failed_import_run_row(
                state,
                finished_at=provisional_finished_at,
                report=provisional_report_by_field,
                error_text=error_text,
            )
        ],
        PTG2ImportRun,
        rewrite=True,
    )
    if state.manifest_stage_table is not None:
        await _drop_ptg2_snapshot_table_names(
            _ptg2_manifest_stage_table_names(state.manifest_stage_table),
            snapshot_id=state.snapshot_id,
            internal_run_id=state.import_run_id,
        )


async def _persist_final_failure(
    state: _FailedImportPersistence,
    report_by_field: dict[str, Any],
    timing_by_metric: dict[str, Any],
    *,
    error_text: str,
    persistence_started_monotonic: float,
) -> None:
    if state.import_started_monotonic is None:
        return
    _finalize_failure_report(
        state,
        report_by_field,
        timing_by_metric,
        persistence_started_monotonic=persistence_started_monotonic,
    )
    final_finished_at = _utcnow()
    await _push_ptg2_objects(
        [
            _failed_import_run_row(
                state,
                finished_at=final_finished_at,
                report=report_by_field,
                error_text=error_text,
            )
        ],
        PTG2ImportRun,
        rewrite=True,
    )


async def _mark_ptg2_import_failed(
    state: _FailedImportPersistence,
) -> dict[str, Any] | None:
    """Persist failed import state and return its report, or None on failure."""

    error_text = str(state.error)
    report_by_field = dict(state.report or {})
    report_by_field.setdefault("snapshot_id", state.snapshot_id)
    timing_by_metric = dict(report_by_field.get("timings") or {})
    timing_by_metric.pop("total_seconds", None)
    report_by_field["timings"] = timing_by_metric
    persistence_started_monotonic = _ptg2_monotonic()
    try:
        async with db.transaction():
            await _persist_provisional_failure(
                state,
                report_by_field,
                error_text=error_text,
            )
            await _persist_final_failure(
                state,
                report_by_field,
                timing_by_metric,
                error_text=error_text,
                persistence_started_monotonic=(persistence_started_monotonic),
            )
        return report_by_field
    except Exception as mark_exc:
        if is_stale_metadata_fence_error(mark_exc):
            raise_stale_metadata_fence(mark_exc)
        logger.error(
            "Failed to mark PTG2 import %s as failed: %s",
            state.import_run_id,
            mark_exc,
        )
        return None


async def _is_failed_shared_layout_abandoned(
    shared_layout_reservation: Any,
    *,
    build_token: str,
    expected_generation: str = PTG2_V3_SHARED_GENERATION,
    progress_callback: Callable[[str, int], None] | None = None,
) -> bool | None:
    """Abandon an owned unpublished layout, or defer interrupted cleanup to GC."""
    if shared_layout_reservation is None or shared_layout_reservation.reused:
        return None
    for attempt in range(3):
        try:
            if expected_generation == PTG2_V4_SHARED_GENERATION:
                abandonment = await abandon_owned_v4_layout(
                    snapshot_key=shared_layout_reservation.snapshot_key,
                    build_token=build_token,
                    progress_callback=progress_callback,
                )
                return abandonment.logical_layout_count == 1
            async with db.transaction() as session:
                return await is_shared_layout_build_abandoned(
                    session,
                    schema_name=resolve_ptg2_schema(),
                    snapshot_key=shared_layout_reservation.snapshot_key,
                    build_token=build_token,
                )
        except PTG2SharedLayoutAbandonmentDeferred:
            logger.warning(
                "Bounded PTG V4 failed-layout cleanup deferred to recurring GC",
                exc_info=True,
            )
            return None
        except Exception:
            if attempt == 2:
                logger.warning(
                    "Failed to abandon unpublished shared PTG layout after retries; "
                    "recurring GC will retry",
                    exc_info=True,
                )
                return None
            await asyncio.sleep(0.1 * (2**attempt))
    return None


async def _cleanup_failed_ptg2_source_state(
    *,
    serving_index: dict[str, Any] | None,
    snapshot_id: str,
    internal_run_id: str,
) -> None:
    """Remove unpublished relational, artifact, and source-dictionary state."""
    try:
        await _drop_ptg2_snapshot_tables_for_manifest(serving_index)
    except Exception as exc:
        if is_stale_metadata_fence_error(exc):
            raise_stale_metadata_fence(exc)
        logger.debug(
            "Failed to clean PTG2 source-scoped tables for failed import",
            exc_info=True,
        )
    try:
        await delete_ptg2_artifacts_for_snapshot(
            snapshot_id,
            import_run_id=internal_run_id,
        )
    except Exception as exc:
        if is_stale_metadata_fence_error(exc):
            raise_stale_metadata_fence(exc)
        logger.debug("Failed to clean PTG2 artifacts for failed import", exc_info=True)
    try:
        await delete_unpublished_snapshot_sources(
            schema_name=resolve_ptg2_schema(),
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
    except Exception as exc:
        if is_stale_metadata_fence_error(exc):
            raise_stale_metadata_fence(exc)
        logger.debug(
            "Failed to clean PTG2 shared source metadata for failed import",
            exc_info=True,
        )
    try:
        await _delete_allowed_snapshot_rows(
            snapshot_id,
            internal_run_id=internal_run_id,
        )
    except Exception as exc:
        if is_stale_metadata_fence_error(exc):
            raise_stale_metadata_fence(exc)
        logger.debug(
            "Failed to clean PTG2 allowed-amount rows for failed import",
            exc_info=True,
        )


def _source_version_summary(source_version: PTG2SourceVersion | None) -> dict[str, Any]:
    if source_version is None:
        return {}
    return {
        "engine_source_identity_hash": source_version.source_identity_hash,
        "engine_source_file_version_id": source_version.source_file_version_id,
        "canonical_url": source_version.canonical_url,
        "raw_sha256": source_version.raw_sha256,
        "logical_sha256": source_version.logical_sha256,
        "logical_hash_deferred": source_version.logical_hash_deferred,
        "content_length": source_version.content_length,
        "raw_byte_count": source_version.raw_byte_count,
        "etag": source_version.etag,
        "last_modified": source_version.last_modified,
        "verification_mode": getattr(
            source_version,
            "verification_mode",
            None,
        ),
    }


def _raw_job_dedupe_screen_line(
    job: Mapping[str, Any],
    downloaded: PTG2DownloadedJob,
) -> str:
    """Render duplicate evidence without exposing protected source identities."""

    display_label = _ptg_job_display_label(job)
    line = "PTG2_RAW_JOB_DEDUPE" f"\ttype={job.get('type')}" f"\ttarget={display_label}"
    if not job.get("_ptg_progress_private"):
        line += (
            f"\traw_sha256={downloaded.raw_artifact.raw_sha256}"
            f"\tlogical_sha256={downloaded.logical_artifact.logical_sha256}"
        )
    return line + "\treason=duplicate_logical_artifact"


def _ptg_job_display_label(job: Mapping[str, Any]) -> str:
    return str(job.get("_ptg_progress_label") or job.get("url") or "PTG file")


def _source_file_versions_from_results(
    files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    versions: list[dict[str, Any]] = []
    seen_version_keys: set[tuple[str | None, str | None]] = set()
    for file_result in files:
        summary = (
            file_result.get("summary")
            if isinstance(file_result.get("summary"), dict)
            else {}
        )
        version_id = summary.get("engine_source_file_version_id") or summary.get(
            "source_file_version_id"
        )
        identity_hash = summary.get("engine_source_identity_hash") or summary.get(
            "source_identity_hash"
        )
        if not version_id and not identity_hash:
            continue
        key = (
            str(version_id) if version_id else None,
            str(identity_hash) if identity_hash else None,
        )
        if key in seen_version_keys:
            continue
        seen_version_keys.add(key)
        versions.append(
            {
                "source_type": file_result.get("source_type"),
                "url": file_result.get("url"),
                "file_id": file_result.get("file_id"),
                "engine_source_identity_hash": identity_hash,
                "engine_source_file_version_id": version_id,
                "canonical_url": summary.get("canonical_url") or file_result.get("url"),
                "raw_sha256": summary.get("raw_sha256"),
                "logical_sha256": summary.get("logical_sha256"),
                "logical_hash_deferred": bool(summary.get("logical_hash_deferred")),
                "content_length": summary.get("content_length"),
                "raw_byte_count": summary.get("raw_byte_count"),
                "etag": summary.get("etag"),
                "last_modified": summary.get("last_modified"),
                "verification_mode": summary.get("verification_mode"),
            }
        )
    return versions


def _frozen_rate_file_proof(
    options_by_name: Mapping[str, Any],
    file_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return exact per-part proof when this import used a frozen file set."""

    frozen_rate_files = options_by_name.get("frozen_rate_files")
    if not isinstance(frozen_rate_files, list) or not frozen_rate_files:
        return []
    return validate_frozen_processed_results(
        frozen_rate_files,
        file_results,
    )


def _frozen_publication_fields(
    options_by_name: Mapping[str, Any],
    proof_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Carry the complete protected tuple and binding into durable evidence."""

    frozen_binding_by_name = options_by_name.get(FROZEN_RATE_FILE_BINDING_OPTION)
    frozen_rate_files = options_by_name.get("frozen_rate_files")
    if (
        not isinstance(frozen_binding_by_name, Mapping)
        or not isinstance(frozen_rate_files, list)
        or not proof_rows
    ):
        return {}
    return {
        "source_file_import_id": frozen_binding_by_name.get("source_file_import_id"),
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": [
            dict(frozen_descriptor) for frozen_descriptor in frozen_rate_files
        ],
        "frozen_rate_file_set_sha256": options_by_name.get(
            "frozen_rate_file_set_sha256"
        ),
        "frozen_rate_file_count": options_by_name.get("frozen_rate_file_count"),
        "frozen_rate_file_proof": [dict(proof_row) for proof_row in proof_rows],
        "frozen_rate_file_proof_sha256": (
            frozen_rate_file_proof_sha256([dict(proof_row) for proof_row in proof_rows])
        ),
        FROZEN_RATE_FILE_BINDING_OPTION: dict(frozen_binding_by_name),
    }


_ALLOWED_AMOUNT_METRIC_KEYS = (
    "allowed_amount_plans",
    "allowed_amount_items",
    "allowed_amount_blocks",
    "allowed_amount_payments",
    "allowed_amount_provider_payments",
    "allowed_amount_npi_references",
    "allowed_amount_unique_tins",
)


async def _current_allowed_snapshot_id(source_key: str) -> str | None:
    """Resolve the current allowed-evidence snapshot for one logical source."""

    return await _current_source_snapshot_id(_allowed_source_pointer_key(source_key))


def _allowed_amount_metrics_from_results(
    file_results: Iterable[Mapping[str, Any]],
) -> dict[str, int | bool]:
    metrics_by_name: dict[str, int | bool] = {
        metric_name: 0 for metric_name in _ALLOWED_AMOUNT_METRIC_KEYS
    }
    for file_result in file_results:
        if str(file_result.get("source_type") or "") != "allowed_amounts":
            continue
        summary = file_result.get("summary")
        if not isinstance(summary, Mapping):
            continue
        for metric_name in _ALLOWED_AMOUNT_METRIC_KEYS:
            try:
                metric_value = max(0, int(summary.get(metric_name) or 0))
            except (TypeError, ValueError):
                continue
            metrics_by_name[metric_name] = (
                int(metrics_by_name.get(metric_name) or 0) + metric_value
            )
    metrics_by_name["allowed_amount_evidence"] = bool(
        int(metrics_by_name.get("allowed_amount_provider_payments") or 0) > 0
    )
    return metrics_by_name


async def _delete_allowed_snapshot_rows(
    snapshot_id: str,
    *,
    internal_run_id: str | None = None,
) -> None:
    """Delete all unpublished allowed-amount rows owned by one snapshot."""

    resolved_schema_name = resolve_ptg2_schema()
    schema_name = _quote_ident(resolved_schema_name)
    async with db.transaction() as session:
        await lock_writable_snapshot(
            session,
            db,
            schema_name=resolved_schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        for table_name in PTG2_ALLOWED_AMOUNT_TABLE_NAMES:
            await session.execute(
                db.text(
                    f"""
                    DELETE FROM {schema_name}.{_quote_ident(table_name)}
                     WHERE snapshot_id = :snapshot_id
                    """
                ),
                {"snapshot_id": snapshot_id},
            )


def _allowed_amount_index_manifest(
    allowed_metrics: Mapping[str, Any],
    *,
    source_key: str,
    previous_snapshot_id: str | None,
) -> dict[str, Any]:
    schema_name = resolve_ptg2_schema()
    return {
        "contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "snapshot_scoped": True,
        "data_domain": PTG2_DOMAIN_ALLOWED_AMOUNT,
        "source_key": source_key,
        "current_source_key": _allowed_source_pointer_key(source_key),
        "previous_snapshot_id": previous_snapshot_id,
        "tables": {
            "plans": f"{schema_name}.ptg2_allowed_amount_plan",
            "items": f"{schema_name}.ptg2_allowed_amount_item",
            "payments": f"{schema_name}.ptg2_allowed_amount_payment",
            "provider_payments": (
                f"{schema_name}.ptg2_allowed_amount_provider_payment"
            ),
        },
        **{
            metric_name: int(allowed_metrics.get(metric_name) or 0)
            for metric_name in _ALLOWED_AMOUNT_METRIC_KEYS
        },
        "allowed_amount_evidence": bool(allowed_metrics.get("allowed_amount_evidence")),
    }


def _normalize_source_network_names(value: Any) -> list[str]:
    names: list[str] = []
    seen_names: set[str] = set()
    for raw_value in _as_list(value):
        name = str(raw_value or "").strip()
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        names.append(name)
    return names


_PTG2_SNAPSHOT_SET_OPTION_KEYS = (
    "plan_ids",
    "plan_name_contains",
    "plan_market_types",
    "file_url_contains",
)

_PTG2_SNAPSHOT_CONTENT_OPTION_KEYS = (
    "toc_urls",
    "toc_list",
    "in_network_url",
    "allowed_url",
    "source_key",
    *_PTG2_SNAPSHOT_SET_OPTION_KEYS,
    "source_network_names",
    "max_files",
    "snapshot_arch",
    "storage_generation",
    "test_mode",
)

_PTG2_FROZEN_SNAPSHOT_CONTENT_OPTION_KEYS = (
    "source_file_import_id",
    "frozen_rate_file_set_contract",
    "frozen_rate_file_set_sha256",
    "frozen_rate_file_count",
    FROZEN_RATE_FILE_BINDING_OPTION,
)


def _ptg2_snapshot_content_options(option_by_name: dict[str, Any]) -> dict[str, Any]:
    content_option_by_name = {
        key: option_by_name.get(key) for key in _PTG2_SNAPSHOT_CONTENT_OPTION_KEYS
    }
    if isinstance(
        option_by_name.get(FROZEN_RATE_FILE_BINDING_OPTION),
        Mapping,
    ):
        content_option_by_name.update(
            {
                key: option_by_name.get(key)
                for key in _PTG2_FROZEN_SNAPSHOT_CONTENT_OPTION_KEYS
            }
        )
    rebuild_scope_digest = normalized_full_rebuild_scope_digest(
        option_by_name.get("full_rebuild_scope_digest")
    )
    if rebuild_scope_digest is not None:
        content_option_by_name["full_rebuild_scope_digest"] = rebuild_scope_digest
    content_option_by_name["toc_urls"] = _dedupe_preserve(
        [
            str(toc_url_value).strip()
            for toc_url_value in _as_list(option_by_name.get("toc_urls"))
            if str(toc_url_value).strip()
        ]
    )
    for key in _PTG2_SNAPSHOT_SET_OPTION_KEYS:
        content_option_by_name[key] = sorted(
            set(_normalize_filter_values(option_by_name.get(key)))
        )
    content_option_by_name["source_network_names"] = sorted(
        set(
            _normalize_source_network_names(option_by_name.get("source_network_names"))
        ),
        key=str.casefold,
    )
    return content_option_by_name


def _ptg2_deterministic_snapshot_id(
    *,
    import_month: datetime.date,
    import_id: str,
    option_by_name: dict[str, Any],
) -> str:
    identity_by_field = {
        "identity_version": 2,
        "import_id": import_id,
        "import_month": import_month.isoformat(),
        "content_options": _ptg2_snapshot_content_options(option_by_name),
    }
    identity_bytes = canonical_json_dumps(
        {"domain": "ptg2_snapshot_identity_v2", "payload": identity_by_field}
    ).encode("utf-8")
    identity_hash = hash_prefix(sha256_bytes(identity_bytes), 12)
    return f"ptg2:{import_month.strftime('%Y%m')}:{identity_hash}"


def _ptg2_import_run_id(
    import_id: str,
    *,
    full_rebuild_scope_digest: str | None = None,
) -> str:
    """Keep legacy run identity unless one controlled rebuild needs isolation."""

    legacy_run_id = f"ptg2:{import_id}"
    rebuild_scope_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if rebuild_scope_digest is None:
        return legacy_run_id
    rebuild_suffix = f":rebuild-{rebuild_scope_digest[:24]}"
    prefix_length = 96 - len(rebuild_suffix)
    return f"{legacy_run_id[:prefix_length]}{rebuild_suffix}"


def _published_snapshot_manifest(snapshot_attributes: dict[str, Any]) -> dict[str, Any]:
    manifest = snapshot_attributes.get("manifest")
    if isinstance(manifest, dict):
        return manifest
    if isinstance(manifest, str):
        try:
            parsed = json.loads(manifest)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _published_snapshot_serving_index(
    snapshot_attributes: dict[str, Any],
) -> dict[str, Any]:
    serving_index = _published_snapshot_manifest(snapshot_attributes).get(
        "serving_index"
    )
    return dict(serving_index) if isinstance(serving_index, dict) else {}


async def _reconcile_already_published_snapshot(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
) -> dict[str, Any]:
    """Repair every current pointer represented by a published manifest."""

    manifest = _published_snapshot_manifest(snapshot_attributes)
    serving_index = _published_snapshot_serving_index(snapshot_attributes)
    pointer_reconciliation_by_field = await _reconcile_serving_snapshot_pointer(
        snapshot_attributes=snapshot_attributes,
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        serving_index=serving_index,
    )

    allowed_amount_index = manifest.get("allowed_amount_index")
    if (
        isinstance(allowed_amount_index, Mapping)
        and allowed_amount_index.get("contract") == PTG2_ALLOWED_AMOUNT_CONTRACT
    ):
        allowed_previous_snapshot_id = allowed_amount_index.get("previous_snapshot_id")
        allowed_pointer_result = await _reconcile_allowed_snapshot_pointer(
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=(
                str(allowed_previous_snapshot_id)
                if allowed_previous_snapshot_id
                else None
            ),
            import_month=import_month,
        )
        if pointer_reconciliation_by_field is None:
            return allowed_pointer_result
        pointer_reconciliation_by_field["allowed_amount_pointer"] = (
            allowed_pointer_result
        )
    if pointer_reconciliation_by_field is None:
        return {
            "status": "not_applicable",
            "reason": "snapshot has no current-source lifecycle",
        }
    return pointer_reconciliation_by_field


async def _reconcile_serving_snapshot_pointer(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    serving_index: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Repair the negotiated serving pointer when the snapshot owns one."""

    if serving_index.get("storage") != "manifest_snapshot":
        return None
    previous_snapshot_id = snapshot_attributes.get("previous_snapshot_id")
    current_snapshot_id = await _current_source_snapshot_id(source_key)
    allowed_current_ids = {
        snapshot_id,
        str(previous_snapshot_id) if previous_snapshot_id else None,
        None,
    }
    if current_snapshot_id not in allowed_current_ids:
        return {
            "status": "superseded",
            "source_key": source_key,
            "snapshot_id": snapshot_id,
            "current_snapshot_id": current_snapshot_id,
        }
    schema_name = resolve_ptg2_schema()
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        missing_tables, missing_artifacts = await _missing_snapshot_serving_resources(
            schema_name,
            snapshot_id,
            dict(serving_index),
        )
        if missing_tables or missing_artifacts:
            missing_resources = [*missing_tables, *missing_artifacts]
            raise RuntimeError(
                "Published PTG snapshot serving resources are missing: "
                + ", ".join(missing_resources)
            )
        return await _publish_ptg2_source_pointers(
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=(
                str(previous_snapshot_id) if previous_snapshot_id else None
            ),
            import_month=import_month,
            updated_at=_utcnow(),
            snapshot_attributes=snapshot_attributes,
        )


async def _publish_allowed_current_pointer(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
) -> dict[str, Any]:
    """Advance the isolated allowed-evidence current pointer."""

    pointer_source_key = _allowed_source_pointer_key(source_key)
    schema_name = resolve_ptg2_schema()
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        await _compare_and_swap_source_pointer(
            session,
            schema_name=schema_name,
            source_key=pointer_source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            import_month=import_month,
            updated_at=updated_at,
        )
    return {
        "status": "promoted",
        "source_key": pointer_source_key,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
    }


async def _publish_mixed_candidate_current_pointers(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    previous_allowed_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Atomically activate negotiated and allowed pointers for one candidate."""

    schema_name = resolve_ptg2_schema()
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        negotiated_pointer_result = (
            await _activate_ptg2_source_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key=source_key,
                snapshot_id=snapshot_id,
                expected_current_snapshot_id=previous_snapshot_id,
            )
        )
    allowed_pointer_result = negotiated_pointer_result.get("allowed_amount_pointer")
    if not isinstance(allowed_pointer_result, dict):
        raise RuntimeError(
            "mixed candidate activation did not publish its allowed pointer"
        )
    expected_allowed_source_key = _allowed_source_pointer_key(source_key)
    if (
        allowed_pointer_result.get("source_key") != expected_allowed_source_key
        or allowed_pointer_result.get("snapshot_id") != snapshot_id
        or allowed_pointer_result.get("previous_snapshot_id")
        != previous_allowed_snapshot_id
    ):
        raise RuntimeError(
            "mixed candidate activation returned an inconsistent allowed pointer"
        )
    return negotiated_pointer_result, allowed_pointer_result


async def _reconcile_allowed_snapshot_pointer(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
) -> dict[str, Any]:
    """Repair an allowed current pointer for an idempotent published rerun."""

    current_snapshot_id = await _current_allowed_snapshot_id(source_key)
    allowed_current_ids = {
        snapshot_id,
        previous_snapshot_id,
        None,
    }
    if current_snapshot_id not in allowed_current_ids:
        return {
            "status": "superseded",
            "source_key": _allowed_source_pointer_key(source_key),
            "snapshot_id": snapshot_id,
            "current_snapshot_id": current_snapshot_id,
        }
    return await _publish_allowed_current_pointer(
        source_key=source_key,
        snapshot_id=snapshot_id,
        previous_snapshot_id=previous_snapshot_id,
        import_month=import_month,
        updated_at=_utcnow(),
    )


@dataclass(frozen=True)
class _CandidateReuseState:
    manifest: dict[str, Any]
    activation: dict[str, Any]
    serving_index: dict[str, Any]
    allowed_amount_index: Mapping[str, Any] | None


async def _validated_candidate_reuse_state(
    snapshot_attributes: dict[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
) -> _CandidateReuseState:
    manifest = _published_snapshot_manifest(snapshot_attributes)
    activation = manifest.get("activation")
    if (
        not isinstance(activation, dict)
        or activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT
        or activation.get("state") != "validated"
    ):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} is validated without the "
            "strict V3 candidate contract"
        )
    if str(activation.get("source_key") or "") != source_key:
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} candidate source does not "
            f"match {source_key}"
        )
    serving_index = manifest.get("serving_index")
    if not isinstance(serving_index, dict):
        raise RuntimeError(f"PTG snapshot {snapshot_id} candidate has no serving index")
    missing_tables, missing_artifacts = await _missing_snapshot_serving_resources(
        resolve_ptg2_schema(),
        snapshot_id,
        serving_index,
    )
    if missing_tables or missing_artifacts:
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} candidate resources are missing: "
            + ", ".join([*missing_tables, *missing_artifacts])
        )
    raw_allowed_index = manifest.get("allowed_amount_index")
    allowed_amount_index = (
        raw_allowed_index
        if isinstance(raw_allowed_index, Mapping)
        and raw_allowed_index.get("contract") == PTG2_ALLOWED_AMOUNT_CONTRACT
        else None
    )
    return _CandidateReuseState(
        manifest=manifest,
        activation=activation,
        serving_index=serving_index,
        allowed_amount_index=allowed_amount_index,
    )


async def _activate_reused_candidate(
    state: _CandidateReuseState,
    snapshot_attributes: dict[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    previous_snapshot_id = state.activation.get("expected_previous_snapshot_id")
    normalized_previous_id = str(previous_snapshot_id) if previous_snapshot_id else None
    activated_at = _utcnow()
    if state.allowed_amount_index is not None:
        allowed_previous_id = state.allowed_amount_index.get("previous_snapshot_id")
        return await _publish_mixed_candidate_current_pointers(
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=normalized_previous_id,
            previous_allowed_snapshot_id=(
                str(allowed_previous_id) if allowed_previous_id else None
            ),
            import_month=import_month,
            updated_at=activated_at,
        )
    pointer_result = await _publish_ptg2_source_pointers(
        source_key=source_key,
        snapshot_id=snapshot_id,
        previous_snapshot_id=normalized_previous_id,
        import_month=import_month,
        updated_at=activated_at,
        snapshot_attributes=activated_snapshot_attributes(
            snapshot_attributes,
            activated_at=activated_at,
            activation_mode="automatic_redelivery",
        ),
    )
    return pointer_result, None


def _reused_candidate_result(
    state: _CandidateReuseState,
    snapshot_attributes: Mapping[str, Any],
    *,
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    auto_activate: bool,
    pointer_result: Mapping[str, Any] | None,
    allowed_pointer_result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    rate_count = state.manifest.get(
        "serving_rates",
        state.manifest.get(
            "rate_count",
            state.serving_index.get(
                "serving_rates",
                state.serving_index.get("rate_count"),
            ),
        ),
    )
    source_file_versions = state.manifest.get("source_file_versions")
    result_by_field = {
        "status": "succeeded",
        "arch_version": "postgres_binary_v3",
        "publish_status": (
            "candidate_activated" if auto_activate else "candidate_validated"
        ),
        "activation_status": "activated" if auto_activate else "deferred",
        "snapshot_status": (
            PTG2_STATUS_PUBLISHED if auto_activate else PTG2_STATUS_VALIDATED
        ),
        "already_published": False,
        "candidate_reused": True,
        "import_run_id": str(snapshot_attributes.get("import_run_id") or ""),
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "import_month": import_month.isoformat(),
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "pointer_reconciliation": pointer_result,
        "allowed_amount_pointer": allowed_pointer_result,
        "source_file_versions": (
            list(source_file_versions) if isinstance(source_file_versions, list) else []
        ),
        **_frozen_manifest_result_fields(state.manifest),
    }
    if state.allowed_amount_index is not None:
        result_by_field["allowed_amount_index"] = dict(state.allowed_amount_index)
        result_by_field.update(
            _published_allowed_metrics(
                state.manifest,
                state.allowed_amount_index,
            )
        )
    return result_by_field


async def _resume_validated_candidate(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    auto_activate: bool,
) -> dict[str, Any]:
    """Return or atomically activate an idempotently redelivered candidate."""

    state = await _validated_candidate_reuse_state(
        snapshot_attributes,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )
    pointer_result: dict[str, Any] | None = None
    allowed_pointer_result: dict[str, Any] | None = None
    if auto_activate:
        pointer_result, allowed_pointer_result = await _activate_reused_candidate(
            state,
            snapshot_attributes,
            snapshot_id=snapshot_id,
            source_key=source_key,
            import_month=import_month,
        )
    return _reused_candidate_result(
        state,
        snapshot_attributes,
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        auto_activate=auto_activate,
        pointer_result=pointer_result,
        allowed_pointer_result=allowed_pointer_result,
    )


_PTG2_MANIFEST_STAGE_SUPPORT_KINDS = (
    "price_atom",
    "price_set_atom",
    "price_set_summary",
)


def _ptg2_manifest_stage_table_names(serving_stage_table: str) -> list[str]:
    return [
        serving_stage_table,
        *(
            _ptg2_manifest_support_stage_table(serving_stage_table, kind)
            for kind in _PTG2_MANIFEST_STAGE_SUPPORT_KINDS
        ),
    ]


def _already_published_result(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    import_run_id: str,
    source_key: str,
    import_month: datetime.date,
    pointer_reconciliation: dict[str, Any],
) -> dict[str, Any]:
    """Build an idempotent success result from one published snapshot."""

    manifest = _published_snapshot_manifest(snapshot_attributes)
    serving_index = manifest.get("serving_index")
    serving_index = serving_index if isinstance(serving_index, dict) else {}
    rate_count = manifest.get(
        "serving_rates",
        manifest.get(
            "rate_count",
            serving_index.get("serving_rates", serving_index.get("rate_count")),
        ),
    )
    allowed_amount_index = manifest.get("allowed_amount_index")
    allowed_amount_index = (
        allowed_amount_index if isinstance(allowed_amount_index, dict) else {}
    )
    allowed_metrics_by_name = _published_allowed_metrics(
        manifest,
        allowed_amount_index,
    )
    has_allowed_amount_snapshot = (
        allowed_amount_index.get("contract") == PTG2_ALLOWED_AMOUNT_CONTRACT
    )
    return {
        "status": "succeeded",
        "publish_status": "already_published",
        "already_published": True,
        "message": (
            "PTG allowed-amount snapshot is already published"
            if has_allowed_amount_snapshot
            else "PTG snapshot is already published; serving pointers were reconciled"
        ),
        "import_run_id": str(snapshot_attributes.get("import_run_id") or import_run_id),
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "import_month": import_month.isoformat(),
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "arch_version": manifest.get("arch_version"),
        "activation_status": manifest.get("activation_status"),
        "snapshot_status": PTG2_STATUS_PUBLISHED,
        "source_file_versions": manifest.get("source_file_versions") or [],
        **_frozen_manifest_result_fields(manifest),
        **allowed_metrics_by_name,
        "address_refresh": manifest.get("address_refresh"),
        "pointer_reconciliation": pointer_reconciliation,
    }


def _published_allowed_metrics(
    manifest: Mapping[str, Any],
    allowed_amount_index: Mapping[str, Any],
) -> dict[str, int | bool]:
    allowed_metrics_by_name: dict[str, int | bool] = {
        metric_name: int(
            manifest.get(
                metric_name,
                allowed_amount_index.get(metric_name, 0),
            )
            or 0
        )
        for metric_name in _ALLOWED_AMOUNT_METRIC_KEYS
    }
    allowed_metrics_by_name["allowed_amount_evidence"] = bool(
        manifest.get(
            "allowed_amount_evidence",
            allowed_amount_index.get("allowed_amount_evidence", False),
        )
    )
    return allowed_metrics_by_name


_SHARED_V3_PHYSICAL_SERVING_INDEX_KEYS = frozenset(
    {
        "storage",
        "type",
        "snapshot_scoped",
        "arch_version",
        "storage_generation",
        "cold_lookup_contract",
        "price_membership_semantics",
        "serving_multiplicity_semantics",
        "shared_snapshot_key",
        "provider_scope_strategy",
        "id_storage",
        "serving_table_layout",
        "shared_block_layout",
        "source_count",
        "code_count",
        "serving_rates",
        "rate_count",
        "atom_key_bits",
        "price_atom_constant_keys",
        "price_atom_constant_values",
        "price_stage",
        "serving_binary",
        "provider_graph",
        "provider_identifier_quarantine",
        "finalizer_block_copy",
        "storage_bytes",
        "timings",
        "audit_sample",
        "source_witness",
        "snapshot_map",
    }
)


def _shared_reuse_generation(expected_generation: str) -> tuple[str, str]:
    generation = str(expected_generation or "").strip().lower()
    if generation not in {
        PTG2_V3_SHARED_GENERATION,
        PTG2_V4_SHARED_GENERATION,
    }:
        raise RuntimeError("reusable shared layout requested an unsupported generation")
    expected_layout = (
        "packed_snapshot_maps_v4"
        if generation == PTG2_V4_SHARED_GENERATION
        else "dense_shared_blocks_v3"
    )
    return generation, expected_layout


def _validate_reused_shared_contract(
    serving_index: Mapping[str, Any],
    *,
    generation: str,
    expected_layout: str,
) -> None:
    if (
        str(serving_index.get("arch_version") or "").strip().lower()
        != "postgres_binary_v3"
    ):
        raise RuntimeError("reusable strict V3 layout has an incompatible architecture")
    if (
        str(serving_index.get("storage_generation") or "").strip().lower() != generation
        or str(serving_index.get("cold_lookup_contract") or "").strip().lower()
        != PTG2_V3_COLD_LOOKUP_CONTRACT
        or str(serving_index.get("price_membership_semantics") or "").strip().lower()
        != PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS
        or str(serving_index.get("serving_multiplicity_semantics") or "")
        .strip()
        .lower()
        != PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
        or str(serving_index.get("shared_block_layout") or "").strip().lower()
        != expected_layout
    ):
        raise RuntimeError(
            "reusable strict V3 layout is missing the shared cold-read contract"
        )


def _validate_reused_v4_contract(serving_index: Mapping[str, Any]) -> None:
    snapshot_map = serving_index.get("snapshot_map")
    serving_binary = serving_index.get("serving_binary")
    provider_graph = (
        serving_binary.get("provider_graph_v4")
        if isinstance(serving_binary, Mapping)
        else None
    )
    if (
        serving_index.get("type") != "ptg2_shared_blocks_v4"
        or serving_index.get("provider_scope_strategy") != "postgres_packed_graph_v4"
        or not isinstance(snapshot_map, Mapping)
        or not isinstance(provider_graph, Mapping)
        or provider_graph.get("contract") != "ptg2_provider_graph_v4"
    ):
        raise RuntimeError(
            "reusable PTG V4 layout is missing its packed graph contract"
        )


def _validated_reused_source_evidence(
    serving_index: dict[str, Any],
) -> None:
    try:
        source_count = int(serving_index.get("source_count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("reusable strict V3 layout is missing source_count") from exc
    if source_count <= 0:
        raise RuntimeError("reusable strict V3 layout has an invalid source_count")
    serving_index["source_count"] = source_count
    try:
        serving_index["source_witness"] = validate_source_witness_manifest(
            serving_index.get("source_witness"),
            expected_source_count=source_count,
        )
    except ValueError as exc:
        raise RuntimeError(
            "reusable strict V3 layout has incompatible " "source witness evidence"
        ) from exc
    try:
        serving_index["provider_identifier_quarantine"] = (
            validate_provider_identifier_quarantine(
                serving_index.get("provider_identifier_quarantine")
            )
        )
    except ValueError as exc:
        raise RuntimeError(
            "reusable strict V3 layout has invalid provider "
            "identifier quarantine evidence"
        ) from exc


def _validate_reused_code_count(serving_index: Mapping[str, Any]) -> None:
    try:
        code_count = int(serving_index.get("code_count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("reusable strict V3 layout is missing code_count") from exc
    if code_count < 0:
        raise RuntimeError("reusable strict V3 layout has an invalid code_count")


def _reused_shared_v3_serving_index(
    layout_manifest: Mapping[str, Any] | None,
    *,
    source_key: str,
    shared_snapshot_key: int,
    expected_generation: str = PTG2_V3_SHARED_GENERATION,
) -> dict[str, Any]:
    """Bind source-scoped metadata to one already sealed physical layout."""

    layout_manifest_map = dict(layout_manifest or {})
    raw_serving_index = layout_manifest_map.get("serving_index", layout_manifest_map)
    if not isinstance(raw_serving_index, Mapping):
        raise RuntimeError("reusable strict V3 layout is missing its serving manifest")
    serving_index = {
        key: raw_serving_index[key]
        for key in _SHARED_V3_PHYSICAL_SERVING_INDEX_KEYS
        if key in raw_serving_index
    }
    generation, expected_layout = _shared_reuse_generation(expected_generation)
    _validate_reused_shared_contract(
        serving_index,
        generation=generation,
        expected_layout=expected_layout,
    )
    if generation == PTG2_V4_SHARED_GENERATION:
        _validate_reused_v4_contract(serving_index)
    _validated_reused_source_evidence(serving_index)
    _validate_reused_code_count(serving_index)
    serving_index.update(
        {
            "source_key": source_key,
            "shared_snapshot_key": int(shared_snapshot_key),
            "storage_generation": generation,
            "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
            "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
            "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
            "serving_binary_table": None,
            "table": None,
            "materialized_tables": {},
        }
    )
    return serving_index


def _bind_v3_entry_identity(
    entries: Any,
    *,
    identity_payload: Mapping[str, str],
    label: str,
) -> list[dict[str, Any]]:
    """Bind one homogeneous V3 metadata list to a physical source identity."""

    if not isinstance(entries, list):
        raise RuntimeError(f"strict V3 {label} metadata must be a list")
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError(f"strict V3 {label} metadata must contain objects")
        for field_name, identity_value in identity_payload.items():
            previous = entry.setdefault(field_name, identity_value)
            if previous != identity_value:
                raise RuntimeError(
                    f"strict V3 {label} entry has conflicting physical identity"
                )
    return entries


def _bind_provider_metadata_contract(
    copy_files_by_kind: dict[str, Any],
    *,
    identity_payload: Mapping[str, str],
    source_run_contract_sha256: str,
) -> None:
    provider_metadata_entries = _bind_v3_entry_identity(
        copy_files_by_kind.get("provider_set_metadata") or [],
        identity_payload=identity_payload,
        label="provider-set metadata",
    )
    for provider_metadata_entry in provider_metadata_entries:
        existing_digest = provider_metadata_entry.setdefault(
            "source_run_contract_sha256",
            source_run_contract_sha256,
        )
        if existing_digest != source_run_contract_sha256:
            raise RuntimeError(
                "strict V3 provider-set metadata has a conflicting "
                "source-run contract"
            )
    copy_files_by_kind["provider_set_metadata"] = provider_metadata_entries


def _annotate_v3_result_identity(
    file_result: PTG2FileProcessResult,
    identity: SharedPhysicalArtifactIdentity,
    artifact_metadata: Mapping[str, Any],
) -> PTG2FileProcessResult:
    """Attach post-scan physical identity without changing scanner scheduling."""

    if not file_result.success or not isinstance(file_result.summary, dict):
        return file_result
    manifest = file_result.summary.get("manifest")
    if not isinstance(manifest, dict):
        raise RuntimeError("strict V3 successful file result is missing its manifest")
    identity_payload = identity.as_dict()
    manifest["physical_artifact_identity"] = identity_payload
    manifest["logical_artifact_provenance"] = dict(artifact_metadata)
    copy_files = manifest.get("copy_files")
    if not isinstance(copy_files, dict):
        if file_result.skipped:
            return file_result
        raise RuntimeError("strict V3 successful scan is missing deferred COPY files")
    serving_entries = _bind_v3_entry_identity(
        copy_files.get("serving_run") or [],
        identity_payload=identity_payload,
        label="serving-run",
    )
    scanner = file_result.summary.get("scanner")
    scanner_summary = scanner.get("summary") if isinstance(scanner, Mapping) else None
    scanner_config = scanner.get("config") if isinstance(scanner, Mapping) else None
    if not isinstance(scanner_summary, Mapping) or not isinstance(
        scanner_config, Mapping
    ):
        # Synthetic callers may annotate before scanner metadata is assembled. The
        # strict finalizer still rejects these entries because they lack a contract.
        return file_result
    contracted_serving_entries = attach_v3_source_run_contract(
        serving_entries,
        source_identity=identity,
        scanner_summary=scanner_summary,
        scanner_config=scanner_config,
    )
    copy_files["serving_run"] = contracted_serving_entries
    source_run_contract_sha256 = str(
        contracted_serving_entries[0].get("source_run_contract_sha256") or ""
    )
    copy_files["serving_code_dictionary"] = attach_v3_dictionary_contract(
        copy_files.get("serving_code_dictionary") or [],
        source_identity=identity,
        source_run_contract_sha256=source_run_contract_sha256,
        scanner_summary=scanner_summary,
    )
    _bind_provider_metadata_contract(
        copy_files,
        identity_payload=identity_payload,
        source_run_contract_sha256=source_run_contract_sha256,
    )
    return file_result


_annotate_v3_file_result_source_identity = _annotate_v3_result_identity


def _shared_v3_identity_traces(
    file_results: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for file_result in file_results:
        summary = file_result.get("summary")
        manifest = summary.get("manifest") if isinstance(summary, Mapping) else None
        if not isinstance(manifest, Mapping):
            continue
        identity_payload = manifest.get("physical_artifact_identity")
        artifact_metadata = manifest.get("logical_artifact_provenance")
        source_trace_hash = str(manifest.get("source_trace_hash") or "").strip()
        if (
            not isinstance(identity_payload, Mapping)
            or not isinstance(artifact_metadata, Mapping)
            or not source_trace_hash
        ):
            raise RuntimeError(
                "strict V3 logical source result is missing identity/trace metadata"
            )
        pairs.append(
            {
                **normalized_physical_artifact_identity(identity_payload).as_dict(),
                **dict(artifact_metadata),
                "source_trace_hash": source_trace_hash,
            }
        )
    return pairs


_shared_v3_identity_trace_pairs_from_results = _shared_v3_identity_traces


def _shared_v3_provider_identifier_quarantine(
    file_results: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    payloads: list[Mapping[str, Any]] = []
    for file_result in file_results:
        if file_result.get("skipped"):
            continue
        summary = file_result.get("summary")
        scanner = summary.get("scanner") if isinstance(summary, Mapping) else None
        scanner_summary = (
            scanner.get("summary") if isinstance(scanner, Mapping) else None
        )
        payload = (
            scanner_summary.get("provider_identifier_quarantine")
            if isinstance(scanner_summary, Mapping)
            else None
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError(
                "strict V3 scanner omitted provider identifier quarantine evidence"
            )
        payloads.append(payload)
    if not payloads:
        raise RuntimeError(
            "strict V3 publication has no provider identifier quarantine evidence"
        )
    return combine_provider_identifier_quarantines(payloads)


def _sum_v4_tin_only_audits(
    source_file_results: Iterable[Mapping[str, Any]],
) -> int:
    """Combine exact V4 normalization counts across scanned source files."""

    normalization_total = 0
    observed_source_count = 0
    for source_file_result in source_file_results:
        if source_file_result.get("skipped"):
            continue
        file_summary = source_file_result.get("summary")
        scanner_record = (
            file_summary.get("scanner") if isinstance(file_summary, Mapping) else None
        )
        scanner_summary = (
            scanner_record.get("summary")
            if isinstance(scanner_record, Mapping)
            else None
        )
        normalization_audit = (
            scanner_summary.get("empty_npi_tin_only_normalization")
            if isinstance(scanner_summary, Mapping)
            else None
        )
        source_normalization_count = _verify_v4_tin_only_audit(normalization_audit)
        normalization_total += source_normalization_count
        observed_source_count += 1
        if normalization_total > 2**63 - 1:
            raise RuntimeError("PTG V4 empty-NPI normalization count overflow")
    if observed_source_count == 0:
        raise RuntimeError("PTG V4 publication has no empty-NPI normalization evidence")
    return normalization_total


def _shared_v3_source_set_metadata(
    identity_trace_pairs: Iterable[Mapping[str, Any]],
    *,
    expected_source_count: int,
) -> dict[str, Any]:
    """Seal the distinct raw containers from the complete publication input."""

    raw_hashes = {
        str(pair.get("raw_container_sha256") or "").strip().lower()
        for pair in identity_trace_pairs
    }
    metadata = shared_source_set_metadata(raw_hashes)
    if int(metadata["source_count"]) != int(expected_source_count):
        raise RuntimeError(
            "strict V3 source-set seal does not match the complete physical input"
        )
    return metadata


async def _publish_shared_v3_source_dictionary(
    *,
    shared_input_identity: Any,
    identity_trace_pairs: Iterable[Mapping[str, Any]],
    snapshot_id: str,
    expected_source_set: Mapping[str, Any],
) -> tuple[Any, ...]:
    assignments, trace_set_rows = shared_snapshot_source_assignments(
        identity_trace_pairs,
        expected_identities=shared_input_identity.source_identities,
    )
    now = _utcnow()
    await _push_ptg2_objects(
        [{**trace_set_row, "created_at": now} for trace_set_row in trace_set_rows],
        PTG2SourceTraceSet,
        rewrite=True,
    )
    published_source_records = await publish_shared_v3_snapshot_sources(
        schema_name=resolve_ptg2_schema(),
        snapshot_id=snapshot_id,
        plan_scopes=shared_input_identity.logical_plans,
        coverage_scope_id=shared_input_identity.coverage_scope_id,
        assignments=assignments,
    )
    published_source_set = shared_source_set_metadata(
        source_record["raw_container_sha256"]
        for source_record in published_source_records
    )
    if published_source_set != dict(expected_source_set):
        raise RuntimeError(
            "strict V3 logical snapshot source-set seal changed during publication"
        )
    return assignments


async def _publish_shared_v3_plan_rows(
    *,
    shared_input_identity: Any,
    snapshot_id: str,
    import_month: datetime.date,
) -> None:
    """Persist every logical plan that will bind to one physical V3 layout."""

    for plan_fields in shared_input_identity.logical_plan_fields_by_scope:
        plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(
            dict(plan_fields),
            snapshot_id,
            import_month,
        )
        await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
        if alias_rows:
            await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
        await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)


def _is_shared_v3_preflight_eligible(
    downloaded_jobs: Sequence[PTG2DownloadedJob],
) -> bool:
    """Return whether downloads carry enough metadata for a scan-free rebind."""

    if not downloaded_jobs:
        return False
    for downloaded in downloaded_jobs:
        if (
            downloaded.error
            or downloaded.raw_artifact is None
            or downloaded.logical_artifact is None
            or str(downloaded.job.get("type") or "").strip().lower() != "in_network"
        ):
            return False
        job = downloaded.job
        if not logical_plan_fields_for_job(job):
            return False
    return True


_shared_v3_preflight_eligible = _is_shared_v3_preflight_eligible


def _finalizer_block_copy_terminal_metrics(
    finalizer_block_copy: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Flatten allowlisted integer COPY totals for controlled-run proof."""

    terminal_name_by_copy_metric = {
        "source_copy_bytes": "finalizer_block_source_copy_bytes",
        "staged_copy_bytes": "finalizer_block_staged_copy_bytes",
        "source_payload_bytes": "finalizer_block_source_payload_bytes",
        "staged_payload_bytes": "finalizer_block_staged_payload_bytes",
        "reused_payload_bytes": "finalizer_block_reused_payload_bytes",
        "durable_reused_payload_bytes": (
            "finalizer_block_durable_reused_payload_bytes"
        ),
        "same_copy_reused_payload_bytes": (
            "finalizer_block_same_copy_reused_payload_bytes"
        ),
        "row_count": "finalizer_block_row_count",
        "staged_payload_row_count": "finalizer_block_staged_payload_row_count",
        "reused_payload_row_count": "finalizer_block_reused_payload_row_count",
        "durable_reused_row_count": "finalizer_block_durable_reused_row_count",
        "same_copy_reused_row_count": "finalizer_block_same_copy_reused_row_count",
        "unique_block_count": "finalizer_block_unique_block_count",
        "existing_block_count": "finalizer_block_existing_block_count",
        "new_block_count": "finalizer_block_new_block_count",
        "duplicate_block_row_count": "finalizer_block_duplicate_block_row_count",
    }
    total_copy_metrics = (
        finalizer_block_copy.get("total")
        if isinstance(finalizer_block_copy, Mapping)
        else None
    )
    if not isinstance(total_copy_metrics, Mapping):
        return {}
    return {
        terminal_metric_name: metric_value
        for copy_metric_name, terminal_metric_name in (
            terminal_name_by_copy_metric.items()
        )
        if type(metric_value := total_copy_metrics.get(copy_metric_name)) is int
        and metric_value >= 0
    }


def _frozen_manifest_result_fields(
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return frozen multipart proof only when the manifest carries its digest."""

    if not manifest.get("frozen_rate_file_set_sha256"):
        return {}
    return {
        "source_file_import_id": manifest.get("source_file_import_id"),
        "frozen_rate_file_set_contract": manifest.get("frozen_rate_file_set_contract"),
        "frozen_rate_files": manifest.get("frozen_rate_files"),
        "frozen_rate_file_set_sha256": manifest.get("frozen_rate_file_set_sha256"),
        "frozen_rate_file_count": manifest.get("frozen_rate_file_count"),
        "frozen_rate_file_proof": manifest.get("frozen_rate_file_proof"),
        "frozen_rate_file_proof_sha256": manifest.get("frozen_rate_file_proof_sha256"),
        FROZEN_RATE_FILE_BINDING_OPTION: manifest.get(FROZEN_RATE_FILE_BINDING_OPTION),
    }


def _full_rebuild_proof_metrics(
    stage_counts: PTG2ArtifactStageCounts,
    *,
    full_rebuild_scope_digest: str | None,
    shared_layout_reused: bool,
    shared_layout_reused_at_seal: bool,
    finalizer_block_copy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe whether a controlled rebuild reused physical input work."""

    if full_rebuild_scope_digest is None:
        return {}
    metrics_by_name = {
        "full_rebuild": True,
        "artifacts_observed": stage_counts.artifacts_observed,
        "raw_artifacts_total": stage_counts.raw_artifacts_total,
        "raw_artifacts_reused": stage_counts.raw_artifacts_reused,
        "raw_artifacts_unique": stage_counts.raw_artifacts_unique,
        "raw_artifacts_duplicate_identities": (
            stage_counts.raw_artifacts_duplicate_identities
        ),
        "logical_artifacts_total": stage_counts.logical_artifacts_total,
        "logical_artifacts_reused": stage_counts.logical_artifacts_reused,
        "logical_artifacts_unique": stage_counts.logical_artifacts_unique,
        "logical_artifacts_duplicate_identities": (
            stage_counts.logical_artifacts_duplicate_identities
        ),
        "logical_artifacts_deferred_hashes": (
            stage_counts.logical_artifacts_deferred_hashes
        ),
        "shared_layout_reused": bool(shared_layout_reused),
        "shared_layout_reused_at_seal": bool(shared_layout_reused_at_seal),
    }
    metrics_by_name.update(_finalizer_block_copy_terminal_metrics(finalizer_block_copy))
    return metrics_by_name


def _assert_full_rebuild_is_fresh(
    metrics_by_name: Mapping[str, Any],
) -> None:
    """Fail a controlled rebuild if any physical input work was reused."""

    if not metrics_by_name:
        return
    if (
        int(metrics_by_name.get("raw_artifacts_reused") or 0) > 0
        or int(metrics_by_name.get("logical_artifacts_reused") or 0) > 0
        or int(metrics_by_name.get("raw_artifacts_duplicate_identities") or 0) > 0
        or int(metrics_by_name.get("logical_artifacts_duplicate_identities") or 0) > 0
        or bool(metrics_by_name.get("shared_layout_reused"))
        or bool(metrics_by_name.get("shared_layout_reused_at_seal"))
    ):
        raise PTG2FullRebuildFreshnessError(
            "controlled full rebuild selected retained or duplicate work; "
            "create a new attempt after clearing the reuse path",
            metrics_by_name,
        )


@dataclass(frozen=True)
class _ReusedSharedV3AllowedContext:
    successful_files: Sequence[Mapping[str, Any]]
    lane_report_by_field: Mapping[str, Any]
    previous_snapshot_id: str | None
    snapshot_state_by_name: dict[str, bool]


def _shared_v3_publisher_sources(
    source_root: Path,
    *,
    include_provider_graph_v4: bool,
) -> tuple[Path, ...]:
    publisher_sources = (
        source_root / "ptg.py",
        source_root / "ptg_parts" / "rust_scanner.py",
        source_root / "ptg_parts" / "ptg2_manifest_publish.py",
        source_root / "ptg_parts" / "ptg2_provider_quarantine.py",
        source_root / "ptg_parts" / "ptg2_serving_binary_v3.py",
        source_root / "ptg_parts" / "ptg2_serving_binary_v3_code_sets.py",
        source_root / "ptg_parts" / "ptg2_serving_binary_v3_primitives.py",
        source_root / "ptg_parts" / "ptg2_serving_binary_v3_types.py",
        source_root / "ptg_parts" / "ptg2_shared_audit.py",
        source_root / "ptg_parts" / "ptg2_shared_blocks.py",
        source_root / "ptg_parts" / "ptg2_shared_finalize.py",
        source_root / "ptg_parts" / "ptg2_shared_graph.py",
        source_root / "ptg_parts" / "ptg2_shared_price.py",
        source_root / "ptg_parts" / "ptg2_shared_publish.py",
        source_root / "ptg_parts" / "ptg2_shared_snapshot_publish.py",
        source_root / "ptg_parts" / "ptg2_source_witness.py",
        source_root / "ptg_parts" / "ptg2_source_witness_codec.py",
        source_root / "ptg_parts" / "ptg2_source_witness_contract.py",
    )
    if not include_provider_graph_v4:
        return publisher_sources
    return publisher_sources + (
        source_root.parent / "api" / "ptg2_code_filters.py",
        source_root / "ptg_parts" / "ptg2_v4_audit.py",
        source_root / "ptg_parts" / "ptg2_v4_graph_compiler.py",
        source_root / "ptg_parts" / "ptg2_v4_snapshot_maps.py",
        source_root / "ptg_parts" / "ptg2_v4_taxonomy_candidates.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_binding.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_artifact.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_observations.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_projection.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_publish.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_stage.py",
        source_root / "ptg_parts" / "ptg2_tax_identity_source_validation.py",
    )


def _shared_v3_publisher_digest(
    source_root: Path,
    publisher_sources: Sequence[Path],
) -> tuple[str, int]:
    publisher_digest = hashlib.sha256()
    publisher_byte_count = 0
    for source_path in publisher_sources:
        source_bytes = source_path.read_bytes()
        try:
            identity_path = source_path.relative_to(source_root)
        except ValueError:
            identity_path = source_path.relative_to(source_root.parent)
        relative_name = identity_path.as_posix().encode("utf-8")
        publisher_digest.update(len(relative_name).to_bytes(4, "big"))
        publisher_digest.update(relative_name)
        publisher_digest.update(len(source_bytes).to_bytes(8, "big"))
        publisher_digest.update(source_bytes)
        publisher_byte_count += len(source_bytes)
    return publisher_digest.hexdigest(), publisher_byte_count


def _shared_v3_scanner_identity() -> dict[str, Any]:
    """Bind reuse to the exact scanner/finalizer executable that defines output."""

    provider_graph_v4_enabled = _env_bool(
        "HLTHPRT_PTG2_PROVIDER_GRAPH_V4",
        False,
    )
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("strict shared V3 requires the PTG2 Rust scanner binary")
    digest, byte_count = sha256_file(binary)
    source_root = Path(__file__).resolve().parent
    publisher_sources = _shared_v3_publisher_sources(
        source_root,
        include_provider_graph_v4=provider_graph_v4_enabled,
    )
    publisher_digest, publisher_byte_count = _shared_v3_publisher_digest(
        source_root,
        publisher_sources,
    )
    scanner_identity_by_field = {
        "contract_version": 3,
        "scanner_binary_sha256": digest,
        "scanner_binary_bytes": int(byte_count),
        "publisher_source_sha256": publisher_digest,
        "publisher_source_bytes": publisher_byte_count,
    }
    if provider_graph_v4_enabled:
        compiler_binary = _resolve_v4_graph_compiler_binary()
        if compiler_binary is None:
            raise RuntimeError("strict PTG V4 requires its provider graph compiler")
        compiler_digest, compiler_bytes = sha256_file(compiler_binary)
        scanner_identity_by_field.update(
            {
                "contract_version": 4,
                "storage_generation": PTG2_V4_SHARED_GENERATION,
                "provider_graph_compiler_sha256": compiler_digest,
                "provider_graph_compiler_bytes": int(compiler_bytes),
                "provider_graph_encoding_policy": v4_graph_encoding_policy(),
                "inferred_taxonomy_rule_set_sha256": (
                    inferred_provider_taxonomy_rule_set_digest(
                        INFERRED_PROVIDER_TAXONOMY_RULES
                    ).hex()
                ),
            }
        )
    return scanner_identity_by_field


@dataclass(frozen=True)
class _ReusedSharedV3PublicationInputs:
    downloaded_jobs: Sequence[PTG2DownloadedJob]
    shared_input_identity: Any
    classes: Mapping[str, type]
    layout_manifest: Mapping[str, Any] | None
    shared_snapshot_key: int
    semantic_fingerprint: bytes
    coverage_scope_id: bytes
    coverage_plan_scopes: Sequence[Any]
    snapshot_id: str
    import_run_id: str
    source_key: str
    import_month: datetime.date
    previous_snapshot_id: str | None
    started_at: datetime.datetime
    options: Mapping[str, Any]
    allowed_context: _ReusedSharedV3AllowedContext | None
    manifest_stage_table: str | None
    test_mode: bool
    import_started_monotonic: float
    candidate_stage_flags_by_name: dict[str, bool] | None = None
    expected_generation: str = PTG2_V3_SHARED_GENERATION


@dataclass(frozen=True)
class _ReusedSharedV3Evidence:
    allowed_metrics_by_name: dict[str, int | bool]
    source_file_versions: list[dict[str, Any]]
    source_trace_hashes: set[str]
    network_names: set[str]
    source_provenance_entries: list[dict[str, Any]]
    frozen_rate_file_proof: Mapping[str, Any] | None


@dataclass(frozen=True)
class _ReusedSharedV3PublicationState:
    serving_index: dict[str, Any]
    rate_count: int
    auto_activate: bool
    validated_at: datetime.datetime
    timings_by_phase: dict[str, float]
    publish_report_map: dict[str, Any]
    post_publish_started_monotonic: float
    post_publish_stage_timer: _StageTimer


def _validated_allowed_reuse_evidence(
    publication: _ReusedSharedV3PublicationInputs,
) -> tuple[dict[str, int | bool], list[dict[str, Any]]]:
    allowed_context = publication.allowed_context
    if allowed_context is None:
        return {}, []
    allowed_files = [
        dict(file_result) for file_result in allowed_context.successful_files
    ]
    if not allowed_files:
        raise RuntimeError(
            "reused strict V3 mixed publication is missing allowed results"
        )
    allowed_metrics_by_name = _allowed_amount_metrics_from_results(allowed_files)
    if not bool(allowed_metrics_by_name.get("allowed_amount_evidence")):
        raise RuntimeError(
            "reused strict V3 mixed publication has no allowed payment evidence"
        )
    return (
        allowed_metrics_by_name,
        _source_file_versions_from_results(allowed_files),
    )


async def _collect_reused_source_evidence(
    publication: _ReusedSharedV3PublicationInputs,
) -> tuple[
    list[dict[str, Any]],
    set[str],
    set[str],
    list[dict[str, Any]],
]:
    source_file_versions: list[dict[str, Any]] = []
    source_trace_hashes: set[str] = set()
    network_names: set[str] = set()
    source_provenance_entries: list[dict[str, Any]] = []
    for downloaded in publication.downloaded_jobs:
        if (
            downloaded.error
            or downloaded.raw_artifact is None
            or downloaded.logical_artifact is None
        ):
            raise RuntimeError(
                "reusable strict V3 input contains an incomplete download"
            )
        job = downloaded.job
        if str(job.get("type") or "").strip().lower() != "in_network":
            raise RuntimeError(
                "strict V3 fast reuse currently requires in-network-only inputs"
            )
        provenance = await _record_in_network_file_provenance(
            job,
            publication.classes,
            raw_artifact=downloaded.raw_artifact,
            logical_artifact=downloaded.logical_artifact,
            import_run_id=publication.import_run_id,
        )
        source_trace_hash = str(provenance["source_trace_hash"])
        source_trace_hashes.add(source_trace_hash)
        source_provenance_entries.append(
            {
                **shared_physical_artifact_identity(downloaded).as_dict(),
                **shared_logical_artifact_metadata(downloaded),
                "source_trace_hash": source_trace_hash,
            }
        )
        network_names.update(provenance["network_names"])
        source_file_versions.append(
            {
                "source_type": "in_network",
                "url": str(job.get("url") or ""),
                "file_id": provenance["file_row"].get("file_id"),
                **_source_version_summary(provenance["source_version"]),
            }
        )
    return (
        source_file_versions,
        source_trace_hashes,
        network_names,
        source_provenance_entries,
    )


async def _reused_shared_v3_evidence(
    publication: _ReusedSharedV3PublicationInputs,
) -> _ReusedSharedV3Evidence:
    allowed_metrics_by_name, allowed_source_file_versions = (
        _validated_allowed_reuse_evidence(publication)
    )
    (
        source_file_versions,
        source_trace_hashes,
        network_names,
        source_provenance_entries,
    ) = await _collect_reused_source_evidence(publication)
    source_file_versions.extend(allowed_source_file_versions)
    frozen_rate_file_proof = _frozen_rate_file_proof(
        publication.options,
        [
            {
                "source_type": source_version.get("source_type"),
                "url": source_version.get("url"),
                "success": True,
                "summary": source_version,
            }
            for source_version in source_file_versions
        ],
    )
    return _ReusedSharedV3Evidence(
        allowed_metrics_by_name=allowed_metrics_by_name,
        source_file_versions=source_file_versions,
        source_trace_hashes=source_trace_hashes,
        network_names=network_names,
        source_provenance_entries=source_provenance_entries,
        frozen_rate_file_proof=frozen_rate_file_proof,
    )


async def _validate_reused_tax_identity_source_metadata(
    publication: _ReusedSharedV3PublicationInputs,
    serving_index: Mapping[str, Any],
    source_assignments: Iterable[Any],
) -> None:
    """Require sealed source-local evidence for a reused V4 layout."""

    if publication.expected_generation != PTG2_V4_SHARED_GENERATION:
        return
    provider_graph = serving_index.get("provider_graph")
    source_metadata = (
        provider_graph.get("provider_tax_identity_source")
        if isinstance(provider_graph, Mapping)
        else None
    )
    if not isinstance(source_metadata, Mapping):
        raise RuntimeError(
            "reusable PTG V4 layout lacks source-local tax identity evidence"
        )
    binding_index = build_tax_source_bindings(source_assignments)
    expected_bindings = tuple(
        sorted(
            (binding.as_dict() for binding in binding_index.values()),
            key=lambda binding: int(binding["source_key"]),
        )
    )
    await validate_reused_tax_identity_source_projection(
        schema_name=resolve_ptg2_schema(),
        snapshot_key=int(publication.shared_snapshot_key),
        expected_bindings=expected_bindings,
        sealed_metadata=source_metadata,
    )


async def _publish_reused_serving_metadata(
    publication: _ReusedSharedV3PublicationInputs,
    evidence: _ReusedSharedV3Evidence,
) -> dict[str, Any]:
    serving_index = _reused_shared_v3_serving_index(
        publication.layout_manifest,
        source_key=publication.source_key,
        shared_snapshot_key=publication.shared_snapshot_key,
        expected_generation=publication.expected_generation,
    )
    serving_index["coverage_scope_id"] = bytes(publication.coverage_scope_id).hex()
    serving_index["source_trace_set_hash"] = build_source_trace_set(
        sorted(evidence.source_trace_hashes)
    )["source_trace_set_hash"]
    serving_index["network_names"] = sorted(
        evidence.network_names,
        key=str.casefold,
    )
    expected_source_count = publication.shared_input_identity.source_count
    if int(serving_index["source_count"]) != int(expected_source_count):
        raise RuntimeError(
            "reusable strict V3 layout source_count does not match the "
            "complete physical input"
        )
    source_set = _shared_v3_source_set_metadata(
        evidence.source_provenance_entries,
        expected_source_count=expected_source_count,
    )
    source_assignments = await _publish_shared_v3_source_dictionary(
        shared_input_identity=publication.shared_input_identity,
        identity_trace_pairs=evidence.source_provenance_entries,
        snapshot_id=publication.snapshot_id,
        expected_source_set=source_set,
    )
    serving_index["source_set"] = source_set
    await _validate_reused_tax_identity_source_metadata(
        publication,
        serving_index,
        source_assignments,
    )
    await validate_reused_snapshot_sources(
        schema_name=resolve_ptg2_schema(),
        snapshot_key=int(publication.shared_snapshot_key),
        logical_snapshot_id=publication.snapshot_id,
        expected_generation=publication.expected_generation,
    )
    return serving_index


def _reused_shared_v3_publish_report(
    publication: _ReusedSharedV3PublicationInputs,
    evidence: _ReusedSharedV3Evidence,
    serving_index: dict[str, Any],
    timings_by_phase: dict[str, float],
    *,
    auto_activate: bool,
) -> tuple[dict[str, Any], int]:
    """Build the logical snapshot report for a physically reused layout."""

    rate_count = int(
        serving_index.get("serving_rates", serving_index.get("rate_count")) or 0
    )
    report_by_field = {
        "snapshot_id": publication.snapshot_id,
        "source_key": publication.source_key,
        "import_month": publication.import_month.isoformat(),
        "serving_index": serving_index,
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "source_file_versions": evidence.source_file_versions,
        **_frozen_publication_fields(
            publication.options,
            evidence.frozen_rate_file_proof,
        ),
        "shared_layout_reused": True,
        "shared_snapshot_key": int(publication.shared_snapshot_key),
        "shared_semantic_fingerprint": bytes(publication.semantic_fingerprint).hex(),
        "coverage_scope_id": bytes(publication.coverage_scope_id).hex(),
        "activation_status": "activated" if auto_activate else "deferred",
        "data_domains": [
            PTG2_DOMAIN_IN_NETWORK,
            *(
                [PTG2_DOMAIN_ALLOWED_AMOUNT]
                if publication.allowed_context is not None
                else []
            ),
        ],
        "timings": timings_by_phase,
    }
    if publication.allowed_context is not None:
        report_by_field.update(
            {
                "allowed_amount_lane": dict(
                    publication.allowed_context.lane_report_by_field
                ),
                "allowed_amount_index": _allowed_amount_index_manifest(
                    evidence.allowed_metrics_by_name,
                    source_key=publication.source_key,
                    previous_snapshot_id=(
                        publication.allowed_context.previous_snapshot_id
                    ),
                ),
                **evidence.allowed_metrics_by_name,
            }
        )
    return report_by_field, rate_count


def _new_reused_publication_state(
    publication: _ReusedSharedV3PublicationInputs,
    evidence: _ReusedSharedV3Evidence,
    serving_index: dict[str, Any],
) -> _ReusedSharedV3PublicationState:
    post_publish_started_monotonic = _ptg2_monotonic()
    post_publish_seconds_by_stage: dict[str, float] = {}
    stage_timer = _StageTimer(
        post_publish_seconds_by_stage,
        post_publish_started_monotonic,
    )
    timings_by_phase = {
        "data_seconds": 0.0,
        "publish_seconds": 0.0,
        "shared_layout_reuse_seconds": (
            post_publish_started_monotonic - publication.import_started_monotonic
        ),
    }
    auto_activate = bool(publication.options.get("auto_activate_candidates", False))
    publish_report_map, rate_count = _reused_shared_v3_publish_report(
        publication,
        evidence,
        serving_index,
        timings_by_phase,
        auto_activate=auto_activate,
    )
    return _ReusedSharedV3PublicationState(
        serving_index=serving_index,
        rate_count=rate_count,
        auto_activate=auto_activate,
        validated_at=_utcnow(),
        timings_by_phase=timings_by_phase,
        publish_report_map=publish_report_map,
        post_publish_started_monotonic=post_publish_started_monotonic,
        post_publish_stage_timer=stage_timer,
    )


async def _stage_reused_shared_v3_candidate(
    publication: _ReusedSharedV3PublicationInputs,
    state: _ReusedSharedV3PublicationState,
) -> dict[str, Any]:
    snapshot_values_by_field = {
        "snapshot_id": publication.snapshot_id,
        "import_run_id": publication.import_run_id,
        "import_month": publication.import_month,
        "status": PTG2_STATUS_VALIDATED,
        "created_at": publication.started_at,
        "validated_at": state.validated_at,
        "published_at": None,
        "previous_snapshot_id": publication.previous_snapshot_id,
        "manifest": {
            **state.publish_report_map,
            "timings": dict(state.timings_by_phase),
        },
    }
    candidate_result = await _stage_ptg2_source_candidate(
        source_key=publication.source_key,
        snapshot_id=publication.snapshot_id,
        previous_snapshot_id=publication.previous_snapshot_id,
        import_month=publication.import_month,
        updated_at=state.validated_at,
        snapshot_attributes=snapshot_values_by_field,
        shared_snapshot_key=int(publication.shared_snapshot_key),
        coverage_scope_id=bytes(publication.coverage_scope_id),
        coverage_plan_scopes=publication.coverage_plan_scopes,
    )
    if publication.candidate_stage_flags_by_name is not None:
        publication.candidate_stage_flags_by_name["staged"] = True
    return dict(candidate_result["candidate_attributes"])


async def _activate_reused_shared_v3_candidate(
    publication: _ReusedSharedV3PublicationInputs,
    state: _ReusedSharedV3PublicationState,
    candidate_attributes_by_field: dict[str, Any],
) -> str:
    if not state.auto_activate:
        return "deferred"
    activated_at = _utcnow()
    if publication.allowed_context is not None:
        _, state.publish_report_map["allowed_amount_pointer"] = (
            await _publish_mixed_candidate_current_pointers(
                source_key=publication.source_key,
                snapshot_id=publication.snapshot_id,
                previous_snapshot_id=publication.previous_snapshot_id,
                previous_allowed_snapshot_id=(
                    publication.allowed_context.previous_snapshot_id
                ),
                import_month=publication.import_month,
                updated_at=activated_at,
            )
        )
        publication.allowed_context.snapshot_state_by_name["published"] = True
    else:
        await _publish_ptg2_source_pointers(
            source_key=publication.source_key,
            snapshot_id=publication.snapshot_id,
            previous_snapshot_id=publication.previous_snapshot_id,
            import_month=publication.import_month,
            updated_at=activated_at,
            snapshot_attributes=activated_snapshot_attributes(
                candidate_attributes_by_field,
                activated_at=activated_at,
                activation_mode="automatic",
            ),
        )
    return "activated"


async def _complete_reused_shared_v3_publication(
    publication: _ReusedSharedV3PublicationInputs,
    state: _ReusedSharedV3PublicationState,
    *,
    activation_status: str,
) -> Mapping[str, Any]:
    release_current_artifact_lease()
    state.post_publish_stage_timer.mark(
        "logical_candidate_and_optional_pointer_cutover"
    )
    state.post_publish_stage_timer.mark("scratch_cleanup")
    if state.auto_activate:
        await _cleanup_old_ptg2_source_tables(
            publication.source_key,
            {publication.snapshot_id},
            lock_pointer_state=True,
        )
    state.post_publish_stage_timer.mark("old_state_cleanup")
    address_refresh = (
        await _enqueue_address_refresh_after_import(
            source_key=publication.source_key,
            snapshot_id=publication.snapshot_id,
            import_run_id=publication.import_run_id,
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=publication.test_mode,
        )
        if state.auto_activate
        else {"status": "skipped", "reason": "candidate-activation-deferred"}
    )
    state.post_publish_stage_timer.mark("address_refresh")
    state.publish_report_map["address_refresh"] = address_refresh
    state.publish_report_map["activation_status"] = activation_status
    await _persist_completed_ptg2_import_run(
        _CompletedImportPersistence(
            import_run_id=publication.import_run_id,
            snapshot_id=publication.snapshot_id,
            manifest_stage_table=publication.manifest_stage_table,
            import_month=publication.import_month,
            started_at=publication.started_at,
            options=publication.options,
            report_payload=state.publish_report_map,
            timing_payload=state.timings_by_phase,
            import_started_monotonic=publication.import_started_monotonic,
            post_publish_started_monotonic=(state.post_publish_started_monotonic),
            post_publish_stage_timer=state.post_publish_stage_timer,
        )
    )
    return address_refresh


def _reused_shared_v3_result(
    publication: _ReusedSharedV3PublicationInputs,
    evidence: _ReusedSharedV3Evidence,
    state: _ReusedSharedV3PublicationState,
    address_refresh: Mapping[str, Any],
    *,
    activation_status: str,
) -> dict[str, Any]:
    allowed_result_fields = (
        {
            "allowed_amount_lane": dict(
                publication.allowed_context.lane_report_by_field
            ),
            **evidence.allowed_metrics_by_name,
        }
        if publication.allowed_context is not None
        else {}
    )
    return {
        "status": "succeeded",
        "publish_status": "shared_layout_reused",
        "already_published": False,
        "shared_layout_reused": True,
        "storage_generation": str(publication.expected_generation),
        "activation_status": activation_status,
        "snapshot_status": (
            PTG2_STATUS_PUBLISHED if state.auto_activate else PTG2_STATUS_VALIDATED
        ),
        "shared_snapshot_key": int(publication.shared_snapshot_key),
        "import_run_id": publication.import_run_id,
        "snapshot_id": publication.snapshot_id,
        "source_key": publication.source_key,
        "import_month": publication.import_month.isoformat(),
        "files_attempted": len(publication.downloaded_jobs),
        "files_processed": 0,
        "files_reused": len(publication.downloaded_jobs),
        "files_failed": 0,
        "serving_rates": state.rate_count,
        "rate_count": state.rate_count,
        "source_file_versions": evidence.source_file_versions,
        **allowed_result_fields,
        "address_refresh": address_refresh,
        "timings": state.timings_by_phase,
    }


async def _publish_reused_shared_v3_snapshot(
    **publication_options_by_name: Any,
) -> dict[str, Any]:
    """Publish a logical snapshot binding without rescanning identical content."""

    publication = _ReusedSharedV3PublicationInputs(**publication_options_by_name)
    evidence = await _reused_shared_v3_evidence(publication)
    serving_index = await _publish_reused_serving_metadata(
        publication,
        evidence,
    )
    state = _new_reused_publication_state(
        publication,
        evidence,
        serving_index,
    )
    candidate_attributes_by_field = await _stage_reused_shared_v3_candidate(
        publication, state
    )
    activation_status = await _activate_reused_shared_v3_candidate(
        publication,
        state,
        candidate_attributes_by_field,
    )
    address_refresh = await _complete_reused_shared_v3_publication(
        publication,
        state,
        activation_status=activation_status,
    )
    write_live_progress(
        status="succeeded",
        phase="succeeded",
        unit="files",
        done=len(publication.downloaded_jobs),
        total=len(publication.downloaded_jobs),
        pct=100,
        eta_seconds=0,
        message="PTG import reused an identical PostgreSQL layout",
    )
    return _reused_shared_v3_result(
        publication,
        evidence,
        state,
        address_refresh,
        activation_status=activation_status,
    )


@dataclass(frozen=True)
class _AllowedFileProcessingContext:
    classes: dict[str, type]
    test_mode: bool
    reuse_raw_artifacts: bool
    max_bytes: int | None
    max_items: int | None
    import_run_id: str
    snapshot_id: str
    keep_partial_artifacts: bool | None


@dataclass(frozen=True)
class _AllowedSnapshotPublishContext:
    snapshot_id: str
    import_run_id: str
    source_key: str
    previous_snapshot_id: str | None
    import_month: datetime.date
    started_at: datetime.datetime
    options_by_name: Mapping[str, Any]
    import_started_monotonic: float
    data_started_monotonic: float


@dataclass(frozen=True)
class _AllowedSnapshotPublishPreparation:
    allowed_metrics_by_name: dict[str, int | bool]
    report_by_field: dict[str, Any]
    timing_by_metric: dict[str, Any]
    published_at: datetime.datetime
    publish_started_monotonic: float


async def _load_allowed_file_result(
    downloaded: PTG2DownloadedJob,
    context: _AllowedFileProcessingContext,
) -> PTG2FileProcessResult:
    job_by_field = downloaded.job
    source_url = str(job_by_field.get("url") or "")
    if downloaded.error:
        return PTG2FileProcessResult(
            "allowed_amounts",
            source_url,
            False,
            error=downloaded.error,
        )
    if downloaded.raw_artifact is None or downloaded.logical_artifact is None:
        return PTG2FileProcessResult(
            "allowed_amounts",
            source_url,
            False,
            error="download did not produce both raw and logical artifacts",
        )
    try:
        return await _process_allowed_amounts_file(
            job_by_field,
            context.classes,
            context.test_mode,
            reuse_raw_artifacts=context.reuse_raw_artifacts,
            max_bytes=context.max_bytes,
            max_items=context.max_items,
            import_run_id=context.import_run_id,
            snapshot_id=context.snapshot_id,
            keep_partial_artifacts=context.keep_partial_artifacts,
            raw_artifact=downloaded.raw_artifact,
            logical_artifact=downloaded.logical_artifact,
        )
    except Exception as exc:
        return PTG2FileProcessResult(
            "allowed_amounts",
            source_url,
            False,
            error=str(exc),
        )


def _write_allowed_file_progress(
    successful_file_count: int,
    attempted_file_count: int,
    *,
    progress_start_pct: float = 20.0,
    progress_end_pct: float = 90.0,
) -> None:
    write_live_progress(
        phase="processing allowed amounts",
        unit="files",
        done=successful_file_count,
        total=attempted_file_count,
        pct=min(
            progress_end_pct,
            progress_start_pct
            + (successful_file_count / max(attempted_file_count, 1))
            * (progress_end_pct - progress_start_pct),
        ),
        message=(
            f"processed {successful_file_count} of {attempted_file_count} "
            "allowed-amount file(s)"
        ),
    )


def _validate_allowed_file_results(
    successful_files: list[dict[str, Any]],
    failed_files: list[dict[str, Any]],
    attempted_file_count: int,
) -> None:
    if failed_files:
        raise RuntimeError(
            f"PTG2 allowed-amount import failed {len(failed_files)} of "
            f"{attempted_file_count} attempted file(s); strict V3 never "
            "publishes partial source coverage"
        )
    if not successful_files:
        raise RuntimeError(
            "PTG2 allowed-amount import processed zero files successfully"
        )


def _start_allowed_file_progress(
    attempted_file_count: int,
    progress_start_pct: float,
) -> None:
    write_live_progress(
        phase="processing allowed amounts",
        unit="files",
        done=0,
        total=attempted_file_count,
        pct=progress_start_pct,
        message=(f"processing {attempted_file_count} allowed-amount file(s)"),
    )


async def _process_allowed_snapshot_files(
    selected_jobs: Sequence[dict[str, Any]],
    context: _AllowedFileProcessingContext,
    failure_report_by_field: dict[str, Any],
    artifact_stage_observer: PTG2ArtifactStageObserver | None = None,
    progress_start_pct: float = 20.0,
    progress_end_pct: float = 90.0,
) -> list[dict[str, Any]]:
    """Download and parse the full allowed-only strict-V3 file set."""

    await _delete_allowed_snapshot_rows(context.snapshot_id)
    attempted_file_count = len(selected_jobs)
    successful_files: list[dict[str, Any]] = []
    failed_files: list[dict[str, Any]] = []
    _start_allowed_file_progress(
        attempted_file_count,
        progress_start_pct,
    )
    async for downloaded in _iter_downloaded_ptg_jobs(
        selected_jobs,
        reuse_raw_artifacts=context.reuse_raw_artifacts,
        max_bytes=context.max_bytes,
        keep_partial_artifacts=context.keep_partial_artifacts,
        progress_start_pct=5.0,
        progress_end_pct=progress_start_pct,
        **(
            {"artifact_stage_observer": artifact_stage_observer}
            if artifact_stage_observer is not None
            else {}
        ),
    ):
        file_result = await _load_allowed_file_result(downloaded, context)
        file_by_field = asdict(file_result)
        if file_result.success:
            successful_files.append(file_by_field)
            _write_allowed_file_progress(
                len(successful_files),
                attempted_file_count,
                progress_start_pct=progress_start_pct,
                progress_end_pct=progress_end_pct,
            )
        else:
            failed_files.append(file_by_field)
    failure_report_by_field.update(
        {
            "files_attempted": attempted_file_count,
            "files_processed": len(successful_files),
            "files_failed": len(failed_files),
            "files_skipped": 0,
            "successful_files": successful_files,
            "failed_files": failed_files,
        }
    )
    _validate_allowed_file_results(
        successful_files,
        failed_files,
        attempted_file_count,
    )
    return successful_files


def _allowed_snapshot_report(
    successful_files: list[dict[str, Any]],
    context: _AllowedSnapshotPublishContext,
    allowed_metrics_by_name: Mapping[str, Any],
    source_file_versions: list[dict[str, Any]],
    timing_by_metric: dict[str, Any],
) -> dict[str, Any]:
    report_by_field = {
        "snapshot_id": context.snapshot_id,
        "source_key": context.source_key,
        "import_month": context.import_month.isoformat(),
        "files_attempted": len(successful_files),
        "files_processed": len(successful_files),
        "files_failed": 0,
        "files_skipped": 0,
        "successful_files": successful_files,
        "serving_rates": 0,
        "rate_count": 0,
        "arch_version": "postgres_binary_v3",
        "snapshot_status": PTG2_STATUS_PUBLISHED,
        "activation_status": "not_applicable",
        "data_domains": [PTG2_DOMAIN_ALLOWED_AMOUNT],
        "allowed_amount_index": _allowed_amount_index_manifest(
            allowed_metrics_by_name,
            source_key=context.source_key,
            previous_snapshot_id=context.previous_snapshot_id,
        ),
        "source_file_versions": source_file_versions,
        "address_refresh": {
            "status": "skipped",
            "reason": ("allowed-amount evidence has no provider-price serving rows"),
        },
        **allowed_metrics_by_name,
        "timings": timing_by_metric,
    }
    frozen_set_digest = context.options_by_name.get("frozen_rate_file_set_sha256")
    if frozen_set_digest:
        frozen_proof = _frozen_rate_file_proof(
            context.options_by_name,
            successful_files,
        )
        report_by_field.update(
            _frozen_publication_fields(
                context.options_by_name,
                frozen_proof,
            )
        )
    return report_by_field


async def _persist_allowed_snapshot(
    context: _AllowedSnapshotPublishContext,
    report_by_field: dict[str, Any],
    published_at: datetime.datetime,
) -> None:
    snapshot_state = await _push_ptg2_objects(
        [
            {
                "snapshot_id": context.snapshot_id,
                "import_run_id": context.import_run_id,
                "import_month": context.import_month,
                "status": PTG2_STATUS_PUBLISHED,
                "created_at": context.started_at,
                "validated_at": published_at,
                "published_at": published_at,
                "previous_snapshot_id": context.previous_snapshot_id,
                "manifest": dict(report_by_field),
            }
        ],
        PTG2Snapshot,
        rewrite=True,
    )
    if str((snapshot_state or {}).get("status") or "") != PTG2_STATUS_PUBLISHED:
        raise RuntimeError(
            "PTG2 allowed-amount snapshot publication did not persist "
            "published state"
        )


async def _complete_allowed_import(
    context: _AllowedSnapshotPublishContext,
    report_by_field: dict[str, Any],
    timing_by_metric: dict[str, Any],
) -> None:
    release_current_artifact_lease()
    post_publish_started_monotonic = _ptg2_monotonic()
    post_publish_stage_timer = _StageTimer(
        {},
        post_publish_started_monotonic,
    )
    post_publish_stage_timer.mark("artifact_lease_release")
    await _persist_completed_ptg2_import_run(
        _CompletedImportPersistence(
            import_run_id=context.import_run_id,
            import_month=context.import_month,
            started_at=context.started_at,
            options=context.options_by_name,
            report_payload=report_by_field,
            timing_payload=timing_by_metric,
            import_started_monotonic=context.import_started_monotonic,
            post_publish_started_monotonic=post_publish_started_monotonic,
            post_publish_stage_timer=post_publish_stage_timer,
        )
    )


def _emit_allowed_completion(
    context: _AllowedSnapshotPublishContext,
    successful_file_count: int,
    allowed_payment_count: int,
    total_seconds: float,
) -> None:
    done_line = (
        "PTG2_IMPORT_DONE"
        f"\timport_run_id={context.import_run_id}"
        f"\tsnapshot_id={context.snapshot_id}"
        "\tstatus=published"
        "\tactivation_status=not_applicable"
        f"\tfiles_processed={successful_file_count}"
        "\tfiles_failed=0"
        "\tserving_rates=0"
        f"\tallowed_amount_payments={allowed_payment_count}"
        f"\ttotal_seconds={total_seconds:.2f}"
    )
    _emit_screen_line(done_line)
    logger.info(done_line)


def _allowed_snapshot_result(
    context: _AllowedSnapshotPublishContext,
    report_by_field: Mapping[str, Any],
    timing_by_metric: Mapping[str, Any],
    full_rebuild_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": "succeeded",
        "publish_status": "published_allowed_amounts",
        "arch_version": "postgres_binary_v3",
        "activation_status": "not_applicable",
        "snapshot_status": PTG2_STATUS_PUBLISHED,
        "import_run_id": context.import_run_id,
        "snapshot_id": context.snapshot_id,
        "source_key": context.source_key,
        "import_month": context.import_month.isoformat(),
        "files_attempted": report_by_field["files_attempted"],
        "files_processed": report_by_field["files_processed"],
        "files_failed": 0,
        "files_skipped": 0,
        "serving_rates": 0,
        "rate_count": 0,
        "source_file_versions": report_by_field["source_file_versions"],
        **_frozen_manifest_result_fields(report_by_field),
        "address_refresh": report_by_field["address_refresh"],
        **{
            metric_name: report_by_field[metric_name]
            for metric_name in (*_ALLOWED_AMOUNT_METRIC_KEYS, "allowed_amount_evidence")
        },
        **dict(full_rebuild_metrics or {}),
        "timings": timing_by_metric,
    }


def _prepare_allowed_snapshot_publish(
    successful_files: list[dict[str, Any]],
    context: _AllowedSnapshotPublishContext,
    full_rebuild_metrics: Mapping[str, Any] | None = None,
) -> _AllowedSnapshotPublishPreparation:
    allowed_metrics_by_name = _allowed_amount_metrics_from_results(successful_files)
    if not bool(allowed_metrics_by_name.get("allowed_amount_evidence")):
        raise RuntimeError("PTG2 allowed-amount import produced no payment evidence")
    source_file_versions = _source_file_versions_from_results(successful_files)
    published_at = _utcnow()
    publish_started_monotonic = _ptg2_monotonic()
    timing_by_metric = {
        "setup_seconds": (
            context.data_started_monotonic - context.import_started_monotonic
        ),
        "data_seconds": (publish_started_monotonic - context.data_started_monotonic),
        "publish_seconds": 0.0,
    }
    report_by_field = _allowed_snapshot_report(
        successful_files,
        context,
        allowed_metrics_by_name,
        source_file_versions,
        timing_by_metric,
    )
    if full_rebuild_metrics:
        report_by_field.update(full_rebuild_metrics)
    return _AllowedSnapshotPublishPreparation(
        allowed_metrics_by_name=allowed_metrics_by_name,
        report_by_field=report_by_field,
        timing_by_metric=timing_by_metric,
        published_at=published_at,
        publish_started_monotonic=publish_started_monotonic,
    )


async def _publish_allowed_snapshot(
    successful_files: list[dict[str, Any]],
    context: _AllowedSnapshotPublishContext,
    allowed_snapshot_state_by_name: dict[str, bool],
    full_rebuild_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish durable allowed evidence without creating serving pointers."""

    preparation = _prepare_allowed_snapshot_publish(
        successful_files,
        context,
        full_rebuild_metrics,
    )
    write_live_progress(
        phase="publishing",
        pct=92,
        message="publishing allowed-amount snapshot",
    )
    await _persist_allowed_snapshot(
        context,
        preparation.report_by_field,
        preparation.published_at,
    )
    allowed_snapshot_state_by_name["published"] = True
    await _finish_allowed_snapshot_publish(
        successful_files,
        context,
        preparation,
    )
    return _allowed_snapshot_result(
        context,
        preparation.report_by_field,
        preparation.timing_by_metric,
        full_rebuild_metrics,
    )


async def _finish_allowed_snapshot_publish(
    successful_files: list[dict[str, Any]],
    context: _AllowedSnapshotPublishContext,
    preparation: _AllowedSnapshotPublishPreparation,
) -> None:
    """Finish pointer publication, completion state, and progress reporting."""

    pointer_result = await _publish_allowed_current_pointer(
        source_key=context.source_key,
        snapshot_id=context.snapshot_id,
        previous_snapshot_id=context.previous_snapshot_id,
        import_month=context.import_month,
        updated_at=preparation.published_at,
    )
    preparation.report_by_field["allowed_amount_pointer"] = pointer_result
    preparation.timing_by_metric["publish_seconds"] = (
        _ptg2_monotonic() - preparation.publish_started_monotonic
    )
    await _complete_allowed_import(
        context,
        preparation.report_by_field,
        preparation.timing_by_metric,
    )
    _emit_allowed_completion(
        context,
        len(successful_files),
        int(preparation.allowed_metrics_by_name["allowed_amount_payments"]),
        float(preparation.timing_by_metric["total_seconds"]),
    )
    write_live_progress(
        status="succeeded",
        phase="succeeded",
        unit="files",
        done=len(successful_files),
        total=len(successful_files),
        pct=100,
        eta_seconds=0,
        message="PTG allowed-amount import succeeded",
    )


def _direct_dispatch_plan_info(
    plan_ids: Sequence[str] | None,
    plan_market_types: Sequence[str] | None,
) -> list[dict[str, Any]]:
    normalized_plan_ids = _dedupe_preserve(
        [
            str(plan_id or "").strip()
            for plan_id in (plan_ids or ())
            if str(plan_id or "").strip()
        ]
    )
    normalized_market_types = _dedupe_preserve(
        [
            str(market_type or "").strip().lower()
            for market_type in (plan_market_types or ())
            if str(market_type or "").strip()
        ]
    )
    shared_market_type = (
        normalized_market_types[0] if len(normalized_market_types) == 1 else None
    )
    return [
        {
            "plan_id": plan_id,
            "plan_market_type": shared_market_type,
        }
        for plan_id in normalized_plan_ids
    ]


@dataclass
class _FailedImportCleanupContext:
    serving_index: dict[str, Any] | None
    snapshot_id: str
    import_run_id: str
    source_key: str
    is_known_published: bool
    candidate_staged: bool
    shared_layout_reservation: Any
    shared_layout_build_token: str
    shared_storage_generation: str
    failure_report_by_field: dict[str, Any]
    pending_strict_v3: _PendingStrictV3State


@dataclass
class _AbandonmentProgress:
    failure_report_by_field: dict[str, Any]
    amount_by_metric: dict[str, int] = field(default_factory=dict)

    def report(self, metric: str, amount: int) -> None:
        """Project committed bounded cleanup work into live progress."""

        self.amount_by_metric[metric] = self.amount_by_metric.get(metric, 0) + int(
            amount
        )
        self.failure_report_by_field["shared_layout_abandonment_progress"] = dict(
            sorted(self.amount_by_metric.items())
        )
        write_live_progress(
            phase="failure_cleanup",
            pct=99,
            message=(
                "bounded PTG shared-layout cleanup "
                f"{metric}={self.amount_by_metric[metric]}"
            ),
        )


async def _should_preserve_failed_candidate_tables(
    context: _FailedImportCleanupContext,
) -> bool:
    should_preserve = context.is_known_published or context.candidate_staged
    if should_preserve or context.serving_index is None:
        return should_preserve
    try:
        return (
            await _current_source_snapshot_id(context.source_key) == context.snapshot_id
        )
    except Exception:
        logger.warning(
            "Could not recheck the PTG source pointer during failure handling; "
            "preserving candidate tables to avoid deleting live data",
            exc_info=True,
        )
        return True


async def _abandon_failed_shared_layout(
    context: _FailedImportCleanupContext,
) -> None:
    abandonment_progress = _AbandonmentProgress(context.failure_report_by_field)
    abandoned_layout = await _is_failed_shared_layout_abandoned(
        context.shared_layout_reservation,
        build_token=context.shared_layout_build_token,
        expected_generation=context.shared_storage_generation,
        progress_callback=abandonment_progress.report,
    )
    if abandoned_layout is not None:
        context.failure_report_by_field["shared_layout_abandoned"] = abandoned_layout
    elif (
        context.shared_layout_reservation is not None
        and not context.shared_layout_reservation.reused
    ):
        context.failure_report_by_field["shared_layout_abandoned"] = False
        context.failure_report_by_field["shared_layout_abandonment_deferred"] = True


async def _should_preserve_after_failed_import_cleanup(
    context: _FailedImportCleanupContext,
) -> bool:
    """Clean unpublished artifacts and report whether candidate tables remain."""

    should_preserve = await _should_preserve_failed_candidate_tables(context)
    if not should_preserve:
        await _cleanup_failed_ptg2_source_state(
            serving_index=context.serving_index,
            snapshot_id=context.snapshot_id,
            internal_run_id=context.import_run_id,
        )
        await _abandon_failed_shared_layout(context)
    _cleanup_manifest_copy_entries(context.pending_strict_v3.copy_entries_by_kind)
    _cleanup_strict_v3_graph_artifacts(context.pending_strict_v3.graph_artifacts_map)
    context.pending_strict_v3.copy_entries_by_kind = {}
    context.pending_strict_v3.graph_artifacts_map = {}
    return should_preserve


async def _main_with_artifact_lease(
    test_mode: bool = False,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    source_file_import_id: str | None = None,
    frozen_rate_file_set_contract: str | None = None,
    frozen_rate_files: list[dict[str, Any]] | None = None,
    frozen_rate_file_set_sha256: str | None = None,
    frozen_rate_file_count: int | None = None,
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
    control_attempt_id: str | None = None,
    control_attempt_started_at: str | None = None,
    full_rebuild_scope_digest: str | None = None,
) -> dict[str, Any]:
    """
    PTG2 entry point for the Transparency in Coverage importer.
    """
    import_started_monotonic = _ptg2_monotonic()
    import_month_value = normalize_import_month(import_month)
    source_key_val = _normalize_source_key(
        source_key or os.getenv("HLTHPRT_PTG2_SOURCE_KEY")
    )
    snapshot_arch_version = _ptg2_snapshot_arch_from_env()
    provider_graph_v4_enabled = _env_bool(
        "HLTHPRT_PTG2_PROVIDER_GRAPH_V4",
        False,
    )
    shared_storage_generation = (
        PTG2_V4_SHARED_GENERATION
        if provider_graph_v4_enabled
        else PTG2_V3_SHARED_GENERATION
    )
    direct_frozen_params_by_name = {
        **(
            {
                "source_file_import_id": source_file_import_id,
                "import_id": import_id,
                "source_key": source_key_val,
                "import_month": import_month_value,
                "plan_ids": plan_ids or [],
                "plan_market_types": plan_market_types or [],
            }
            if source_file_import_id is not None
            or any(
                protected_option_value is not None
                for protected_option_value in (
                    frozen_rate_file_set_contract,
                    frozen_rate_files,
                    frozen_rate_file_set_sha256,
                    frozen_rate_file_count,
                )
            )
            else {}
        ),
        **(
            {
                "frozen_rate_file_set_contract": (frozen_rate_file_set_contract),
                "frozen_rate_files": frozen_rate_files,
                "frozen_rate_file_set_sha256": (frozen_rate_file_set_sha256),
                "frozen_rate_file_count": frozen_rate_file_count,
            }
            if any(
                protected_option_value is not None
                for protected_option_value in (
                    frozen_rate_file_set_contract,
                    frozen_rate_files,
                    frozen_rate_file_set_sha256,
                    frozen_rate_file_count,
                )
            )
            else {}
        ),
    }
    normalized_direct_frozen_params_by_name = (
        normalize_protected_frozen_rate_params(direct_frozen_params_by_name)
        if direct_frozen_params_by_name
        else {}
    )
    normalized_frozen_rate_files: list[dict[str, Any]] = []
    normalized_frozen_set_digest: str | None = None
    frozen_binding_by_name = frozen_rate_binding_from_params(
        normalized_direct_frozen_params_by_name
    )
    if protected_frozen_tuple_presence(normalized_direct_frozen_params_by_name):
        normalized_frozen_rate_files = normalized_direct_frozen_params_by_name[
            "frozen_rate_files"
        ]
        normalized_frozen_set_digest = normalized_direct_frozen_params_by_name[
            "frozen_rate_file_set_sha256"
        ]
        assert_frozen_input_compatibility(
            normalized_frozen_rate_files,
            in_network_url=in_network_url,
            allowed_url=allowed_url,
            toc_urls=toc_urls,
            toc_list=toc_list,
            file_url_contains=file_url_contains,
            max_files=max_files,
        )
    rebuild_scope_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if normalized_frozen_set_digest is not None:
        rebuild_scope_digest = bind_frozen_rate_set_to_scope(
            rebuild_scope_digest,
            normalized_frozen_set_digest,
            len(normalized_frozen_rate_files),
        )
    should_reuse_raw_artifacts = (
        reuse_raw_artifacts if rebuild_scope_digest is None else False
    )
    should_keep_partial_artifacts = (
        keep_partial_artifacts if rebuild_scope_digest is None else False
    )
    if provider_ref_url:
        raise ValueError(
            "provider_ref_url is not supported by strict V3; provider references "
            "must come from each in-network source"
        )
    import_id_val = _normalize_import_id(
        (
            str(frozen_binding_by_name["source_file_import_id"])
            if frozen_binding_by_name is not None
            else import_id
        )
        or (
            _frozen_ptg2_import_id(
                import_month_value,
                source_key_val,
                frozen_rate_file_set_sha256=normalized_frozen_set_digest,
                frozen_rate_file_count=len(normalized_frozen_rate_files),
                arch_variant=shared_storage_generation,
            )
            if normalized_frozen_set_digest is not None
            else _default_ptg2_import_id(
                import_month_value,
                source_key_val,
                toc_urls=toc_urls,
                toc_list=toc_list,
                in_network_url=in_network_url,
                allowed_url=allowed_url,
                provider_ref_url=provider_ref_url,
                arch_variant=shared_storage_generation,
            )
        )
    )
    import_run_id = (
        frozen_internal_run_id(str(frozen_binding_by_name["source_file_import_id"]))
        if frozen_binding_by_name is not None
        else _ptg2_import_run_id(
            import_id_val,
            full_rebuild_scope_digest=rebuild_scope_digest,
        )
    )
    if source_key_val is None:
        if test_mode:
            source_key_val = _normalize_source_key(import_id_val)
        else:
            raise ValueError(
                "PTG imports require --source-key or HLTHPRT_PTG2_SOURCE_KEY"
            )
    assert source_key_val is not None
    source_network_name_values = sorted(
        _normalize_source_network_names(source_network_names),
        key=str.casefold,
    )
    should_auto_activate_candidates = _should_auto_activate_ptg2_candidates()
    options_by_name = {
        "toc_urls": toc_urls or [],
        "toc_list": toc_list,
        "in_network_url": in_network_url,
        "allowed_url": allowed_url,
        "source_file_import_id": (
            frozen_binding_by_name.get("source_file_import_id")
            if frozen_binding_by_name is not None
            else source_file_import_id
        ),
        "frozen_rate_file_set_contract": (
            FROZEN_RATE_FILE_SET_CONTRACT
            if frozen_binding_by_name is not None
            else None
        ),
        "frozen_rate_files": normalized_frozen_rate_files,
        "frozen_rate_file_set_sha256": normalized_frozen_set_digest,
        "frozen_rate_file_count": len(normalized_frozen_rate_files),
        "source_key": source_key_val,
        "plan_ids": plan_ids or [],
        "plan_name_contains": plan_name_contains or [],
        "plan_market_types": plan_market_types or [],
        "file_url_contains": file_url_contains or [],
        "source_network_names": source_network_name_values,
        "max_files": max_files,
        "reuse_raw_artifacts": should_reuse_raw_artifacts,
        "keep_partial_artifacts": (
            _env_bool(PTG2_KEEP_PARTIAL_ENV, True)
            if should_keep_partial_artifacts is None
            else should_keep_partial_artifacts
        ),
        "snapshot_arch": snapshot_arch_version,
        "storage_generation": shared_storage_generation,
        "test_mode": test_mode,
        "scanner_workers": max(
            _env_int(PTG2_RUST_WORKERS_ENV, PTG2_DEFAULT_RUST_WORKERS),
            1,
        ),
        "scanner_event_queue": max(
            _env_int(PTG2_RUST_EVENT_QUEUE_ENV, PTG2_DEFAULT_RUST_EVENT_QUEUE),
            1,
        ),
        "file_process_concurrency": max(
            _env_int(PTG2_FILE_PROCESS_CONCURRENCY_ENV, 1),
            1,
        ),
        "price_copy_tasks": max(
            _env_int(
                PTG2_MANIFEST_DIRECT_COPY_TASKS_ENV,
                PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS,
            ),
            1,
        ),
        "auto_activate_candidates": should_auto_activate_candidates,
        **binding_option(frozen_binding_by_name),
    }
    if rebuild_scope_digest is not None:
        options_by_name["full_rebuild_scope_digest"] = rebuild_scope_digest
    snapshot_id = _ptg2_deterministic_snapshot_id(
        import_month=import_month_value,
        import_id=import_id_val,
        option_by_name=options_by_name,
    )
    live_run_id = str(control_run_id or "").strip()
    live_token = set_live_progress_context(
        run_id=live_run_id,
        attempt_id=control_attempt_id,
        attempt_started_at=control_attempt_started_at,
        started_at=control_attempt_started_at,
        source_key=source_key_val,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
    )
    setup_seconds_by_stage: dict[str, float] = {}
    setup_stage_timer = _StageTimer(setup_seconds_by_stage, import_started_monotonic)
    pending_strict_v3 = _PendingStrictV3State({}, {})
    full_rebuild_stage_tracker = PTG2FreshArtifactStageTracker()

    # Enforce a streaming size cap on every caller-supplied URL (never None for
    # control-triggered runs) so a malicious/huge target cannot OOM or fill the node.
    max_bytes = fetch_max_bytes(PTG2_DEFAULT_MAX_BYTES)
    if test_mode:
        raw_max_bytes = os.getenv("HLTHPRT_PTG2_TEST_MAX_BYTES")
        if raw_max_bytes:
            try:
                max_bytes = int(raw_max_bytes)
            except ValueError:
                logger.warning(
                    "Ignoring invalid HLTHPRT_PTG2_TEST_MAX_BYTES=%s", raw_max_bytes
                )
    oversized_frozen_files = [
        descriptor["ordinal"]
        for descriptor in normalized_frozen_rate_files
        if descriptor["content_length"] > max_bytes
    ]
    if oversized_frozen_files:
        raise ValueError(
            "frozen rate file content_length exceeds the configured streaming "
            f"cap at ordinal(s) {oversized_frozen_files}"
        )
    write_live_progress(phase="initializing", pct=1, message="initializing PTG import")
    await ensure_database(test_mode)
    setup_stage_timer.mark("ensure_database")
    await ensure_ptg2_tables()
    setup_stage_timer.mark("ensure_ptg2_tables")
    source_import_lock = _ptg2_source_import_lock(source_key_val)
    has_source_import_lock = False
    try:
        await source_import_lock.__aenter__()
        has_source_import_lock = True
        setup_stage_timer.mark("source_import_lock")
        if normalized_direct_frozen_params_by_name:
            await insert_or_compare_frozen_binding_transaction(
                normalized_direct_frozen_params_by_name
            )
            setup_stage_timer.mark("frozen_source_file_binding")
        observed_source_snapshot_id = await _current_source_snapshot_id(source_key_val)
        observed_allowed_snapshot_id = await _current_allowed_snapshot_id(
            source_key_val
        )
        setup_stage_timer.mark("source_snapshot_lookup")
    except BaseException:
        if has_source_import_lock:
            await source_import_lock.__aexit__(None, None, None)
        reset_live_progress_context(live_token)
        raise
    now = _utcnow()
    initial_import_run_by_field = {
        "import_run_id": import_run_id,
        "import_month": import_month_value,
        "status": PTG2_STATUS_RUNNING,
        "started_at": now,
        "finished_at": None,
        "heartbeat_at": now,
        "options": options_by_name,
        "report": {},
        "error": None,
    }
    try:
        snapshot_state = await _push_ptg2_objects(
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
            initial_import_run_by_field=initial_import_run_by_field,
        )
    except BaseException:
        await source_import_lock.__aexit__(None, None, None)
        reset_live_progress_context(live_token)
        raise
    if snapshot_state and snapshot_state.get("status") == PTG2_STATUS_PUBLISHED:
        try:
            if rebuild_scope_digest is not None:
                raise PTG2FullRebuildFreshnessError(
                    "controlled full rebuild scope already completed; "
                    "create a new attempt",
                    {
                        **_full_rebuild_proof_metrics(
                            full_rebuild_stage_tracker.snapshot(),
                            full_rebuild_scope_digest=(rebuild_scope_digest),
                            shared_layout_reused=False,
                            shared_layout_reused_at_seal=False,
                        ),
                        "existing_snapshot_reused": True,
                    },
                )
            pointer_reconciliation = await _reconcile_already_published_snapshot(
                snapshot_attributes=snapshot_state,
                snapshot_id=snapshot_id,
                source_key=source_key_val,
                import_month=import_month_value,
            )
            already_published_result = _already_published_result(
                snapshot_attributes=snapshot_state,
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
                source_key=source_key_val,
                import_month=import_month_value,
                pointer_reconciliation=pointer_reconciliation,
            )
            await _finalize_resumed_terminal_attempt(
                snapshot_state,
                internal_run_id=import_run_id,
            )
            write_live_progress(
                status="succeeded",
                phase="succeeded",
                pct=100,
                eta_seconds=0,
                message=str(already_published_result["message"]),
            )
            return already_published_result
        finally:
            await source_import_lock.__aexit__(None, None, None)
            reset_live_progress_context(live_token)
    if snapshot_state and snapshot_state.get("status") == PTG2_STATUS_VALIDATED:
        try:
            if rebuild_scope_digest is not None:
                raise PTG2FullRebuildFreshnessError(
                    "controlled full rebuild scope already completed; "
                    "create a new attempt",
                    {
                        **_full_rebuild_proof_metrics(
                            full_rebuild_stage_tracker.snapshot(),
                            full_rebuild_scope_digest=(rebuild_scope_digest),
                            shared_layout_reused=False,
                            shared_layout_reused_at_seal=False,
                        ),
                        "existing_snapshot_reused": True,
                    },
                )
            candidate_result = await _resume_validated_candidate(
                snapshot_attributes=snapshot_state,
                snapshot_id=snapshot_id,
                source_key=source_key_val,
                import_month=import_month_value,
                auto_activate=should_auto_activate_candidates,
            )
            if should_auto_activate_candidates:
                try:
                    await _cleanup_old_ptg2_source_tables(
                        source_key_val,
                        {snapshot_id},
                        lock_pointer_state=True,
                    )
                except Exception:
                    logger.warning(
                        "Validated PTG candidate activated, but old-state cleanup failed",
                        exc_info=True,
                    )
                candidate_result["address_refresh"] = (
                    await _enqueue_address_refresh_after_import(
                        source_key=source_key_val,
                        snapshot_id=snapshot_id,
                        import_run_id=import_run_id,
                        has_serving_files=True,
                        source_scoped_compact=True,
                        test_mode=test_mode,
                    )
                )
            await _finalize_resumed_terminal_attempt(
                snapshot_state,
                internal_run_id=import_run_id,
            )
            write_live_progress(
                status="succeeded",
                phase="succeeded",
                pct=100,
                eta_seconds=0,
                message=(
                    "PTG candidate activated"
                    if should_auto_activate_candidates
                    else "PTG candidate already validated; live pointers unchanged"
                ),
            )
            return candidate_result
        finally:
            await source_import_lock.__aexit__(None, None, None)
            reset_live_progress_context(live_token)
    is_exact_building_retry = bool(
        snapshot_state
        and _is_exact_building_attempt_retry(
            snapshot_state,
            initial_import_run_by_field,
        )
    )
    if (
        snapshot_state
        and snapshot_state.get("snapshot_claim_status") == "existing"
        and not is_exact_building_retry
    ):
        await source_import_lock.__aexit__(None, None, None)
        reset_live_progress_context(live_token)
        existing_status = snapshot_state.get("status") or "<unknown>"
        if existing_status == PTG2_STATUS_BUILDING:
            raise PTG2SnapshotInProgressConflict(
                f"PTG snapshot {snapshot_id} is already being built by "
                f"{snapshot_state.get('import_run_id') or 'another delivery'}"
            )
        raise RuntimeError(
            f"Refusing PTG snapshot claim for {snapshot_id}: existing status is "
            f"{existing_status}"
        )
    failure_report_by_field: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "legacy_table_suffix": import_id_val,
        **(
            {
                "frozen_rate_file_set_sha256": (normalized_frozen_set_digest),
                "frozen_rate_file_count": len(normalized_frozen_rate_files),
            }
            if normalized_frozen_set_digest is not None
            else {}
        ),
    }
    ptg2_manifest_stage_table: str | None = None
    ptg2_import_heartbeat_task: asyncio.Task[Any] | None = None
    shared_layout_reservation = None
    shared_input_identity = None
    shared_layout_build_token = uuid.uuid4().hex
    previous_snapshot_id = (
        str(observed_source_snapshot_id) if observed_source_snapshot_id else None
    )
    previous_allowed_snapshot_id = (
        str(observed_allowed_snapshot_id) if observed_allowed_snapshot_id else None
    )
    is_current_pointer_published = False
    candidate_stage_flags_by_name = {"staged": False}
    allowed_snapshot_state_by_name = {"published": False}

    def failure_cleanup_context(
        serving_index_value: Any,
    ) -> _FailedImportCleanupContext:
        """Capture the current failure-cleanup state without widening closure use."""

        return _FailedImportCleanupContext(
            serving_index=(
                serving_index_value if isinstance(serving_index_value, dict) else None
            ),
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            source_key=source_key_val,
            is_known_published=(
                is_current_pointer_published
                or allowed_snapshot_state_by_name["published"]
            ),
            candidate_staged=candidate_stage_flags_by_name["staged"],
            shared_layout_reservation=shared_layout_reservation,
            shared_layout_build_token=shared_layout_build_token,
            shared_storage_generation=shared_storage_generation,
            failure_report_by_field=failure_report_by_field,
            pending_strict_v3=pending_strict_v3,
        )

    def failure_rebuild_metrics() -> dict[str, Any]:
        """Return controlled-rebuild evidence available at failure time."""

        return _full_rebuild_proof_metrics(
            full_rebuild_stage_tracker.snapshot(),
            full_rebuild_scope_digest=rebuild_scope_digest,
            shared_layout_reused=bool(
                failure_report_by_field.get("shared_layout_reused")
            ),
            shared_layout_reused_at_seal=bool(
                failure_report_by_field.get("shared_layout_reused_at_seal")
            ),
        )

    async def mark_import_failed(
        error: BaseException | str, *, progress_message: str | None = None
    ) -> None:
        """Persist import failure state and drop unpublished source-scoped staging tables."""
        failure_handling_started_monotonic = _ptg2_monotonic()
        error_text = str(error) or "worker task was cancelled"
        failure_report_by_field.update(failure_rebuild_metrics())
        write_live_progress(
            phase="failing",
            pct=99,
            message="persisting PTG import failure state",
        )
        serving_index = failure_report_by_field.get("serving_index")
        should_preserve_candidate_tables = (
            await _should_preserve_after_failed_import_cleanup(
                failure_cleanup_context(serving_index)
            )
        )
        persisted_failure_report = await _mark_ptg2_import_failed(
            _FailedImportPersistence(
                import_run_id=import_run_id,
                snapshot_id=snapshot_id,
                import_month=import_month_value,
                started_at=now,
                error=error_text,
                report=failure_report_by_field,
                options=options_by_name,
                manifest_stage_table=ptg2_manifest_stage_table,
                should_preserve_published_snapshot=(should_preserve_candidate_tables),
                import_started_monotonic=import_started_monotonic,
                failure_handling_started_monotonic=(failure_handling_started_monotonic),
            )
        )
        if persisted_failure_report is None:
            write_live_progress(
                status="failed",
                phase="failure_persistence_incomplete",
                eta_seconds=0,
                message=(
                    "PTG import failed before terminal state committed; "
                    "the exact attempt can be retried"
                ),
            )
            return
        write_live_progress(
            status="failed",
            phase="failed",
            eta_seconds=0,
            message=progress_message or "PTG import failed; inspect worker logs",
        )

    try:
        ptg2_import_heartbeat_task = asyncio.create_task(
            _heartbeat_ptg2_import_run(import_run_id),
            name=f"ptg2-import-heartbeat:{import_run_id}",
        )
        setup_stage_timer.mark("initial_status_rows")
        assert source_key_val is not None
        write_live_progress(phase="planning", pct=3, message="planning PTG files")
        stage_token = _ptg2_snapshot_table_token(source_key_val, snapshot_id)
        classes = await _prepare_ptg_tables(
            import_id_val,
            test_mode,
            initial_table_class_names=set(PTG_CONTROL_TABLE_CLASS_NAMES),
        )
        setup_stage_timer.mark("control_tables")

        direct_frozen_plans = _direct_dispatch_plan_info(
            plan_ids,
            plan_market_types,
        )
        jobs: list[dict[str, Any]] = (
            build_frozen_rate_jobs(
                normalized_frozen_rate_files,
                plan_info=direct_frozen_plans,
                source_network_names=source_network_name_values,
            )
            if normalized_frozen_rate_files
            else []
        )
        data_started_monotonic = _ptg2_monotonic()

        toc_candidates: list[str] = []
        if toc_urls:
            toc_candidates.extend([source_url for source_url in toc_urls if source_url])
        if toc_list:
            toc_candidates.extend(_load_toc_urls_from_file(toc_list))
        toc_candidates = _dedupe_preserve(
            [source_url.strip() for source_url in toc_candidates if source_url.strip()]
        )

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
                    reuse_raw_artifacts=should_reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    keep_partial_artifacts=should_keep_partial_artifacts,
                    raise_on_error=True,
                    **(
                        {
                            "artifact_stage_observer": (
                                full_rebuild_stage_tracker.observe
                            )
                        }
                        if rebuild_scope_digest is not None
                        else {}
                    ),
                )
            except (
                PTG2ArtifactStageFreshnessError,
                PTG2FullRebuildFreshnessError,
            ):
                raise
            except Exception as exc:
                toc_failures.append({"url": toc_url, "error": str(exc)})
                continue
            jobs.extend(toc_jobs)

        if in_network_url:
            direct_job_by_field: dict[str, Any] = {
                "type": "in_network",
                "url": in_network_url,
            }
            direct_in_network_plans = _direct_dispatch_plan_info(
                plan_ids,
                plan_market_types,
            )
            if direct_in_network_plans:
                direct_job_by_field["plan_info"] = direct_in_network_plans
            if source_network_name_values:
                direct_job_by_field["source_network_names"] = source_network_name_values
            jobs.append(direct_job_by_field)
        if allowed_url:
            direct_allowed_job_by_field: dict[str, Any] = {
                "type": "allowed_amounts",
                "url": allowed_url,
            }
            direct_allowed_plans = _direct_dispatch_plan_info(
                plan_ids,
                plan_market_types,
            )
            if direct_allowed_plans:
                direct_allowed_job_by_field["plan_info"] = direct_allowed_plans
            jobs.append(direct_allowed_job_by_field)
        jobs = _filter_jobs_by_url_contains(jobs, file_url_contains)
        if source_network_name_values:
            for job in jobs:
                if job.get(
                    "type"
                ) == "in_network" and not _normalize_source_network_names(
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
        if toc_failures:
            failure_report_by_field = {
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
                "strict V3 never publishes partial source coverage"
            )
        if toc_candidates and not jobs and not in_network_url and not allowed_url:
            failure_report_by_field = {
                "toc_urls": toc_candidates,
                "toc_failures": toc_failures,
                "jobs_discovered": 0,
                "files_attempted": 0,
                "files_processed": 0,
                "files_failed": 0,
                "snapshot_id": snapshot_id,
                "legacy_table_suffix": import_id_val,
            }
            raise RuntimeError(
                "PTG2 import processed table-of-contents input but discovered zero rate files"
            )

        seen_jobs: set[tuple[str, str]] = set()
        selected_supported_jobs: list[dict[str, Any]] = []
        for job in jobs:
            job_key = _ptg_job_identity(job)
            if job_key in seen_jobs:
                continue
            seen_jobs.add(job_key)
            if max_files is not None and len(selected_supported_jobs) >= max_files:
                break
            if job.get("type") in {"in_network", "allowed_amounts"}:
                selected_supported_jobs.append(job)
        if not selected_supported_jobs:
            raise RuntimeError("strict V3 import discovered no supported PTG files")
        selected_jobs = [
            job for job in selected_supported_jobs if job.get("type") == "in_network"
        ]
        allowed_jobs = [
            job
            for job in selected_supported_jobs
            if job.get("type") == "allowed_amounts"
        ]
        failure_report_by_field.update(
            {
                "jobs_discovered": jobs_discovered_before_dedupe,
                "jobs_unique": len(jobs),
                "duplicate_jobs_skipped": duplicate_jobs_skipped,
                "toc_failures": toc_failures,
            }
        )
        successful_allowed_files: list[dict[str, Any]] = []
        allowed_metrics_by_name: dict[str, int | bool] = {}
        allowed_lane_report_by_field: dict[str, Any] = {}
        if allowed_jobs:
            failure_report_by_field["allowed_amount_lane"] = (
                allowed_lane_report_by_field
            )
            processing_context = _AllowedFileProcessingContext(
                classes=classes,
                test_mode=test_mode,
                reuse_raw_artifacts=should_reuse_raw_artifacts,
                max_bytes=max_bytes,
                max_items=max_items,
                import_run_id=import_run_id,
                snapshot_id=snapshot_id,
                keep_partial_artifacts=should_keep_partial_artifacts,
            )
            successful_allowed_files = await _process_allowed_snapshot_files(
                allowed_jobs,
                processing_context,
                allowed_lane_report_by_field,
                progress_start_pct=5.0 if selected_jobs else 20.0,
                progress_end_pct=20.0 if selected_jobs else 90.0,
                **(
                    {"artifact_stage_observer": (full_rebuild_stage_tracker.observe)}
                    if rebuild_scope_digest is not None
                    else {}
                ),
            )
            allowed_metrics_by_name = _allowed_amount_metrics_from_results(
                successful_allowed_files
            )
            if not bool(allowed_metrics_by_name.get("allowed_amount_evidence")):
                raise RuntimeError(
                    "PTG2 allowed-amount import produced no payment evidence"
                )
            if normalized_frozen_rate_files:
                allowed_frozen_proof = _frozen_rate_file_proof(
                    options_by_name,
                    successful_allowed_files,
                )
                failure_report_by_field.update(
                    _frozen_publication_fields(
                        options_by_name,
                        allowed_frozen_proof,
                    )
                )
        pre_rate_rebuild_metrics = _full_rebuild_proof_metrics(
            full_rebuild_stage_tracker.snapshot(),
            full_rebuild_scope_digest=rebuild_scope_digest,
            shared_layout_reused=False,
            shared_layout_reused_at_seal=False,
        )
        _assert_full_rebuild_is_fresh(pre_rate_rebuild_metrics)
        if not selected_jobs:
            publish_context = _AllowedSnapshotPublishContext(
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
                source_key=source_key_val,
                previous_snapshot_id=previous_allowed_snapshot_id,
                import_month=import_month_value,
                started_at=now,
                options_by_name=options_by_name,
                import_started_monotonic=import_started_monotonic,
                data_started_monotonic=data_started_monotonic,
            )
            return await _publish_allowed_snapshot(
                successful_allowed_files,
                publish_context,
                allowed_snapshot_state_by_name,
                full_rebuild_metrics=pre_rate_rebuild_metrics,
            )
        ptg2_manifest_stage_table = _ptg2_manifest_stage_table_name(stage_token)
        await _create_serving_stage_table(
            stage_token,
            snapshot_id=snapshot_id,
            internal_run_id=import_run_id,
            storage_generation=shared_storage_generation,
        )
        setup_stage_timer.mark("manifest_stage_table")
        processed_file_count_map = {"done": 0}
        attempted_files = len(selected_jobs)
        download_start_pct = 20.0 if allowed_jobs else 5.0
        scan_start_pct = 30.0 if allowed_jobs else 20.0
        for progress_index, job in enumerate(selected_jobs):
            job["_ptg_progress_index"] = progress_index
            job["_ptg_progress_total"] = max(attempted_files, 1)
        write_live_progress(
            phase="download",
            unit="files",
            done=0,
            total=attempted_files,
            pct=download_start_pct if attempted_files else scan_start_pct,
            message=f"downloading {attempted_files} PTG file(s)",
        )
        failed_files: list[dict[str, Any]] = []
        skipped_files: list[dict[str, Any]] = []
        successful_files: list[dict[str, Any]] = []
        downloads_by_logical_hash: dict[str, list[PTG2DownloadedJob]] = {}
        duplicate_raw_files_skipped = 0
        file_process_concurrency = 1
        if not test_mode:
            file_process_concurrency = max(
                _env_int(PTG2_FILE_PROCESS_CONCURRENCY_ENV, 1),
                1,
            )
        if file_process_concurrency > 1:
            _emit_screen_line(
                "PTG2_FILE_PROCESS_CONCURRENCY"
                f"\tvalue={file_process_concurrency}"
                f"\tfiles={attempted_files}"
            )
        processing_tasks: set[
            asyncio.Task[tuple[PTG2DownloadedJob, PTG2FileProcessResult | None]]
        ] = set()
        file_progress_coordinator: PTGFileProgressCoordinator | None = None

        async def record_file_result(
            downloaded: PTG2DownloadedJob,
            file_result: PTG2FileProcessResult | None,
        ) -> None:
            """Classify a file result and update completion progress."""
            if file_result is None:
                return
            if file_result.success:
                if file_progress_coordinator is not None:
                    file_progress_coordinator.complete(
                        _progress_job_index(downloaded.job),
                        message=(
                            f"processed {processed_file_count_map['done'] + 1} "
                            f"of {attempted_files} PTG file(s)"
                        ),
                    )
                if file_result.skipped:
                    skipped_files.append(asdict(file_result))
                else:
                    processed_file_count_map["done"] += 1
                    successful_files.append(asdict(file_result))
            else:
                failed_files.append(asdict(file_result))

        async def drain_processing_tasks(*, force: bool = False) -> None:
            """Drain queued processing tasks as capacity requires and record results."""
            if not processing_tasks:
                return
            if force:
                done, pending = await asyncio.wait(
                    processing_tasks, return_when=asyncio.ALL_COMPLETED
                )
            elif len(processing_tasks) < file_process_concurrency:
                return
            else:
                done, pending = await asyncio.wait(
                    processing_tasks, return_when=asyncio.FIRST_COMPLETED
                )
            processing_tasks.clear()
            processing_tasks.update(pending)
            for task in done:
                downloaded, file_result = task.result()
                await record_file_result(downloaded, file_result)

        def file_progress_context(job: dict[str, Any]) -> dict[str, Any]:
            """Attach safe file context without assigning an independent run range."""
            job_index = _progress_job_index(job)
            return {
                **current_live_progress_context(),
                "file_index": job_index + 1,
                "file_count": attempted_files,
                "file_name": str(
                    job.get("_ptg_progress_label") or job.get("url") or ""
                ),
                "private_source": bool(job.get("_ptg_progress_private")),
            }

        async def process_downloaded_job(
            downloaded: PTG2DownloadedJob,
        ) -> tuple[PTG2DownloadedJob, PTG2FileProcessResult | None]:
            """Process a downloaded in-network artifact under its progress context."""
            job = downloaded.job
            token = set_live_progress_context(**file_progress_context(job))
            try:
                if job.get("type") == "in_network":
                    if shared_input_identity is None:
                        raise RuntimeError(
                            "strict V3 physical input identity was not established"
                        )
                    file_result = await _process_in_network_file(
                        _InNetworkFileContext(
                            job=job,
                            classes=classes,
                            test_mode=test_mode,
                            reuse_raw_artifacts=should_reuse_raw_artifacts,
                            max_bytes=max_bytes,
                            max_items=max_items,
                            import_run_id=import_run_id,
                            keep_partial_artifacts=(should_keep_partial_artifacts),
                            snapshot_id=snapshot_id,
                            coverage_scope_id=(
                                shared_input_identity.coverage_scope_hex
                            ),
                            import_month=import_month_value,
                            ptg2_manifest_stage_table=(ptg2_manifest_stage_table),
                            source_network_names=job.get("source_network_names"),
                            raw_artifact=downloaded.raw_artifact,
                            logical_artifact=downloaded.logical_artifact,
                            progress_observer=(
                                file_progress_coordinator.observer(
                                    _progress_job_index(job)
                                )
                                if file_progress_coordinator is not None
                                else None
                            ),
                        )
                    )
                    file_result = _claim_strict_v3_file_result(
                        pending_strict_v3,
                        file_result,
                        shared_physical_artifact_identity(downloaded),
                        shared_logical_artifact_metadata(downloaded),
                    )
                    return downloaded, file_result
                return downloaded, None
            finally:
                reset_live_progress_context(token)

        try:
            buffered_downloads: list[PTG2DownloadedJob] = []
            async for downloaded in _iter_downloaded_ptg_jobs(
                selected_jobs,
                reuse_raw_artifacts=should_reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=should_keep_partial_artifacts,
                progress_start_pct=download_start_pct,
                progress_end_pct=scan_start_pct,
                **(
                    {"artifact_stage_observer": (full_rebuild_stage_tracker.observe)}
                    if rebuild_scope_digest is not None
                    else {}
                ),
            ):
                buffered_downloads.append(downloaded)
            download_rebuild_metrics = _full_rebuild_proof_metrics(
                full_rebuild_stage_tracker.snapshot(),
                full_rebuild_scope_digest=rebuild_scope_digest,
                shared_layout_reused=False,
                shared_layout_reused_at_seal=False,
            )
            _assert_full_rebuild_is_fresh(download_rebuild_metrics)
            download_failures: list[PTG2FileProcessResult] = []
            for downloaded in buffered_downloads:
                if downloaded.error:
                    download_failures.append(
                        PTG2FileProcessResult(
                            str(downloaded.job.get("type") or "unknown"),
                            str(downloaded.job.get("url") or ""),
                            False,
                            error=downloaded.error,
                        )
                    )
                elif (
                    downloaded.raw_artifact is None
                    or downloaded.logical_artifact is None
                ):
                    download_failures.append(
                        PTG2FileProcessResult(
                            str(downloaded.job.get("type") or "unknown"),
                            str(downloaded.job.get("url") or ""),
                            False,
                            error="download did not produce both raw and logical artifacts",
                        )
                    )
            if download_failures:
                failed_files.extend(
                    asdict(failed_download) for failed_download in download_failures
                )
                failure_report_by_field.update(
                    {
                        "files_attempted": attempted_files,
                        "files_processed": 0,
                        "files_failed": len(download_failures),
                        "failed_files": list(failed_files),
                    }
                )
                raise RuntimeError(
                    f"PTG2 import failed {len(download_failures)} of {attempted_files} "
                    "download(s); strict V3 never publishes partial source coverage"
                )
            if not _shared_v3_preflight_eligible(buffered_downloads):
                raise RuntimeError(
                    "strict V3 requires successful in-network downloads with logical plan scope metadata"
                )
            write_live_progress(
                phase="planning",
                unit="files",
                done=len(buffered_downloads),
                total=len(buffered_downloads),
                pct=scan_start_pct,
                message="checking for an identical shared PostgreSQL layout",
            )
            shared_input_identity = shared_physical_input_identity(
                buffered_downloads,
                options=options_by_name,
                scanner_canon_version=_shared_v3_scanner_identity(),
            )
            await _publish_shared_v3_plan_rows(
                shared_input_identity=shared_input_identity,
                snapshot_id=snapshot_id,
                import_month=import_month_value,
            )
            canonical_plan_values_by_field = {
                key: plan_field_value
                for key, plan_field_value in shared_input_identity.logical_plan_fields.items()
                if plan_field_value is not None and str(plan_field_value).strip()
            }
            for downloaded in buffered_downloads:
                job_meta = (
                    dict(downloaded.job.get("meta"))
                    if isinstance(downloaded.job.get("meta"), dict)
                    else {}
                )
                downloaded.job["meta"] = {
                    **job_meta,
                    **canonical_plan_values_by_field,
                }
            async with db.transaction() as session:
                reserve_layout = (
                    reserve_v4_shared_layout
                    if provider_graph_v4_enabled
                    else reserve_shared_layout
                )
                shared_layout_reservation = await reserve_layout(
                    session,
                    schema_name=resolve_ptg2_schema(),
                    semantic_fingerprint=shared_input_identity.semantic_fingerprint,
                    build_token=shared_layout_build_token,
                )
            failure_report_by_field.update(
                {
                    "shared_snapshot_key": shared_layout_reservation.snapshot_key,
                    "shared_semantic_fingerprint": (
                        shared_input_identity.semantic_fingerprint.hex()
                    ),
                    "coverage_scope_id": shared_input_identity.coverage_scope_hex,
                    "shared_layout_reused": shared_layout_reservation.reused,
                    "logical_plan_count": shared_input_identity.logical_plan_count,
                }
            )
            if shared_layout_reservation.reused:
                if rebuild_scope_digest is not None:
                    _assert_full_rebuild_is_fresh(
                        _full_rebuild_proof_metrics(
                            full_rebuild_stage_tracker.snapshot(),
                            full_rebuild_scope_digest=rebuild_scope_digest,
                            shared_layout_reused=True,
                            shared_layout_reused_at_seal=False,
                        )
                    )
                return await _publish_reused_shared_v3_snapshot(
                    downloaded_jobs=buffered_downloads,
                    shared_input_identity=shared_input_identity,
                    classes=classes,
                    layout_manifest=shared_layout_reservation.layout_manifest,
                    shared_snapshot_key=shared_layout_reservation.snapshot_key,
                    semantic_fingerprint=shared_input_identity.semantic_fingerprint,
                    coverage_scope_id=shared_input_identity.coverage_scope_id,
                    coverage_plan_scopes=shared_input_identity.logical_plans,
                    snapshot_id=snapshot_id,
                    import_run_id=import_run_id,
                    source_key=source_key_val,
                    import_month=import_month_value,
                    previous_snapshot_id=previous_snapshot_id,
                    started_at=now,
                    options=options_by_name,
                    allowed_context=(
                        _ReusedSharedV3AllowedContext(
                            successful_files=successful_allowed_files,
                            lane_report_by_field=(allowed_lane_report_by_field),
                            previous_snapshot_id=(previous_allowed_snapshot_id),
                            snapshot_state_by_name=(allowed_snapshot_state_by_name),
                        )
                        if successful_allowed_files
                        else None
                    ),
                    manifest_stage_table=ptg2_manifest_stage_table,
                    test_mode=test_mode,
                    import_started_monotonic=import_started_monotonic,
                    candidate_stage_flags_by_name=candidate_stage_flags_by_name,
                    expected_generation=shared_storage_generation,
                )

            progress_weights: list[int] = [0] * attempted_files
            progress_labels: list[str] = ["PTG file"] * attempted_files
            unique_downloads_by_logical_hash: dict[str, list[PTG2DownloadedJob]] = {}
            for buffered_download in buffered_downloads:
                assert buffered_download.raw_artifact is not None
                assert buffered_download.logical_artifact is not None
                logical_hash = buffered_download.logical_artifact.logical_sha256
                duplicate_physical_input = any(
                    is_same_downloaded_physical_input(previous, buffered_download)
                    for previous in unique_downloads_by_logical_hash.get(
                        logical_hash,
                        (),
                    )
                )
                progress_index = _progress_job_index(buffered_download.job)
                progress_weights[progress_index] = (
                    0
                    if duplicate_physical_input
                    else max(
                        int(
                            getattr(
                                buffered_download.raw_artifact,
                                "byte_count",
                                getattr(
                                    buffered_download.logical_artifact,
                                    "byte_count",
                                    1,
                                ),
                            )
                        ),
                        1,
                    )
                )
                progress_labels[progress_index] = str(
                    buffered_download.job.get("_ptg_progress_label")
                    or buffered_download.job.get("url")
                    or "PTG file"
                )
                if not duplicate_physical_input:
                    unique_downloads_by_logical_hash.setdefault(
                        logical_hash,
                        [],
                    ).append(buffered_download)
            file_progress_coordinator = PTGFileProgressCoordinator(
                progress_weights,
                progress_labels,
                stage_start_pct=scan_start_pct,
                stage_end_pct=90.0,
            )

            async def iter_downloaded_jobs():
                """Yield the fully validated download batch in discovery order."""

                for buffered_download in buffered_downloads:
                    yield buffered_download

            downloaded_jobs = iter_downloaded_jobs()

            async for downloaded in downloaded_jobs:
                job = downloaded.job
                file_result: PTG2FileProcessResult | None = None
                if downloaded.error:
                    logger.warning(
                        "Failed to download %s file %s: %s",
                        job.get("type"),
                        _ptg_job_display_label(job),
                        downloaded.error,
                    )
                    file_result = PTG2FileProcessResult(
                        str(job.get("type") or "unknown"),
                        str(job.get("url") or ""),
                        False,
                        error=downloaded.error,
                    )
                elif (
                    downloaded.raw_artifact is None
                    or downloaded.logical_artifact is None
                ):
                    file_result = PTG2FileProcessResult(
                        str(job.get("type") or "unknown"),
                        str(job.get("url") or ""),
                        False,
                        error="download did not produce an artifact",
                    )
                elif any(
                    is_same_downloaded_physical_input(previous, downloaded)
                    for previous in downloads_by_logical_hash.get(
                        downloaded.logical_artifact.logical_sha256,
                        (),
                    )
                ):
                    duplicate_raw_files_skipped += 1
                    provenance = await _record_in_network_file_provenance(
                        job,
                        classes,
                        raw_artifact=downloaded.raw_artifact,
                        logical_artifact=downloaded.logical_artifact,
                        import_run_id=import_run_id,
                    )
                    _emit_screen_line(_raw_job_dedupe_screen_line(job, downloaded))
                    file_result = PTG2FileProcessResult(
                        str(job.get("type") or "unknown"),
                        str(job.get("url") or ""),
                        True,
                        file_id=int(provenance["file_row"]["file_id"]),
                        summary={
                            **_source_version_summary(provenance["source_version"]),
                            "raw_storage_uri": downloaded.raw_artifact.raw_storage_uri,
                            "reason": "duplicate_logical_artifact",
                            "manifest": {
                                "source_trace_hash": provenance["source_trace_hash"],
                                "source_trace_set_hash": provenance[
                                    "source_trace_set_hash"
                                ],
                                "network_names": provenance["network_names"],
                            },
                        },
                        skipped=True,
                    )
                    file_result = _annotate_v3_file_result_source_identity(
                        file_result,
                        shared_physical_artifact_identity(downloaded),
                        shared_logical_artifact_metadata(downloaded),
                    )
                elif downloaded.logical_artifact.logical_sha256:
                    downloads_by_logical_hash.setdefault(
                        downloaded.logical_artifact.logical_sha256,
                        [],
                    ).append(downloaded)
                if file_result is not None:
                    await record_file_result(downloaded, file_result)
                    continue

                processing_tasks.add(
                    asyncio.create_task(process_downloaded_job(downloaded))
                )
                await drain_processing_tasks()
            await drain_processing_tasks(force=True)
        finally:
            await _cancel_and_wait_tasks(processing_tasks)

        failure_report_by_field = {
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
            **(
                {
                    "frozen_rate_file_set_sha256": (normalized_frozen_set_digest),
                    "frozen_rate_file_count": len(normalized_frozen_rate_files),
                }
                if normalized_frozen_set_digest is not None
                else {}
            ),
        }
        if allowed_jobs:
            failure_report_by_field["allowed_amount_lane"] = (
                allowed_lane_report_by_field
            )
        if shared_layout_reservation is not None:
            failure_report_by_field.update(
                {
                    "shared_snapshot_key": shared_layout_reservation.snapshot_key,
                    "shared_semantic_fingerprint": (
                        shared_input_identity.semantic_fingerprint.hex()
                        if shared_input_identity is not None
                        else None
                    ),
                    "coverage_scope_id": (
                        shared_input_identity.coverage_scope_hex
                        if shared_input_identity is not None
                        else None
                    ),
                    "shared_layout_reused": shared_layout_reservation.reused,
                }
            )
        pending_strict_v3.copy_entries_by_kind = _pending_strict_v3_copy_entries(
            successful_files
        )
        if failed_files:
            raise RuntimeError(
                f"PTG2 import failed {len(failed_files)} of {attempted_files} attempted "
                "file(s); strict V3 never publishes partial source coverage"
            )
        if jobs and processed_file_count_map["done"] == 0:
            raise RuntimeError(
                f"PTG2 import discovered {len(jobs)} job(s) but processed zero files successfully"
            )
        if normalized_frozen_rate_files:
            completed_frozen_proof = _frozen_rate_file_proof(
                options_by_name,
                successful_files + skipped_files,
            )
            failure_report_by_field.update(
                _frozen_publication_fields(
                    options_by_name,
                    completed_frozen_proof,
                )
            )

        if shared_input_identity is None:
            raise RuntimeError(
                "strict V3 source publication is missing physical input identity"
            )
        source_identity_traces = _shared_v3_identity_trace_pairs_from_results(
            successful_files + skipped_files
        )
        source_set = _shared_v3_source_set_metadata(
            source_identity_traces,
            expected_source_count=shared_input_identity.source_count,
        )
        provider_identifier_quarantine = _shared_v3_provider_identifier_quarantine(
            successful_files
        )
        empty_npi_tin_only_normalization_count = (
            _sum_v4_tin_only_audits(successful_files)
            if provider_graph_v4_enabled
            else None
        )
        source_assignments = await _publish_shared_v3_source_dictionary(
            shared_input_identity=shared_input_identity,
            identity_trace_pairs=source_identity_traces,
            snapshot_id=snapshot_id,
            expected_source_set=source_set,
        )

        await flush_error_log(classes["ImportLog"])
        data_seconds = _ptg2_monotonic() - data_started_monotonic
        publish_started_monotonic = _ptg2_monotonic()
        write_live_progress(
            phase="publishing",
            pct=90 if provider_graph_v4_enabled else 92,
            message="publishing PTG snapshot",
        )
        publish_progress_total = 8
        precompile_progress_options = (
            {
                "stage_start_pct": 90.0,
                "stage_end_pct": 92.0,
                "stage_id": "ptg2_v4_precompile",
                "stage_ordinal": 4,
            }
            if provider_graph_v4_enabled
            else {}
        )
        _emit_ptg2_publish_progress(
            "starting",
            completed_steps=0,
            total_steps=publish_progress_total,
            message_text="starting PTG snapshot publish",
            **precompile_progress_options,
        )
        manifest_merge_metrics_by_name: dict[str, Any] = {"enabled": False}
        manifest_precopy_merge_seconds = 0.0
        has_serving_files = any(
            file_summary.get("source_type") == "in_network"
            and not file_summary.get("skipped")
            for file_summary in successful_files
        )
        if not has_serving_files:
            raise RuntimeError(
                "strict V3 import produced no publishable in-network source files"
            )
        strict_v3_copy_entries = pending_strict_v3.copy_entries_by_kind
        if has_serving_files:
            _emit_ptg2_publish_progress(
                "pre-copy merge",
                completed_steps=0,
                total_steps=publish_progress_total,
                message_text="merging manifest copy files before publish",
                **precompile_progress_options,
            )
            manifest_precopy_merge_started_monotonic = _ptg2_monotonic()
            manifest_merge_metrics_by_name = await _merge_ptg2_manifest_files(
                successful_files=successful_files,
                manifest_stage_table=ptg2_manifest_stage_table,
            )
            manifest_precopy_merge_seconds = (
                _ptg2_monotonic() - manifest_precopy_merge_started_monotonic
            )
            manifest_merge_metrics_by_name["elapsed_seconds"] = (
                manifest_precopy_merge_seconds
            )
            _emit_ptg2_publish_progress(
                "pre-copy merge complete",
                completed_steps=4,
                total_steps=publish_progress_total,
                message_text="manifest copy files loaded into staging tables",
                serving_rows=manifest_merge_metrics_by_name.get("serving_rows"),
                streamed_to_copy=manifest_merge_metrics_by_name.get("streamed_to_copy"),
                **precompile_progress_options,
            )
            for file_summary in successful_files:
                summary_payload = (
                    file_summary.get("summary")
                    if isinstance(file_summary, dict)
                    else None
                )
                manifest_payload = (
                    summary_payload.get("manifest")
                    if isinstance(summary_payload, dict)
                    else None
                )
                if isinstance(manifest_payload, dict):
                    manifest_payload.pop("copy_files", None)
        manifest_artifacts = _collect_manifest_artifacts(
            successful_files + skipped_files
        )
        tax_identity_source_artifacts = (
            _bound_tax_identity_source_artifacts(
                successful_files + skipped_files,
                source_assignments,
            )
            if provider_graph_v4_enabled
            else None
        )
        pending_strict_v3.graph_artifacts_map = manifest_artifacts
        assert source_key_val is not None
        if has_serving_files:
            if not ptg2_manifest_stage_table:
                raise RuntimeError(
                    "PTG import did not create a manifest-backed serving stage table"
                )
            _emit_ptg2_publish_progress(
                "publishing snapshot tables",
                completed_steps=5,
                total_steps=publish_progress_total,
                message_text="publishing PTG manifest snapshot tables",
                **precompile_progress_options,
            )
            if shared_layout_reservation is None or shared_input_identity is None:
                raise RuntimeError(
                    "strict V3 publish is missing its physical input reservation"
                )
            run_entries = strict_v3_copy_entries.get("serving_run") or []
            code_dictionary_entries = (
                strict_v3_copy_entries.get("serving_code_dictionary") or []
            )
            provider_set_metadata_entries = (
                strict_v3_copy_entries.get("provider_set_metadata") or []
            )
            source_audit_witness_entries = (
                strict_v3_copy_entries.get("source_audit_witness") or []
            )
            v4_publication_progress = _PTG2V4PublicationProgress()

            def report_snapshot_publication_progress(
                stage_name: str,
                counters_by_name: Mapping[str, int],
            ) -> None:
                """Expose exact V4 publication work without advancing fake time."""

                v4_publication_progress.observe(stage_name, counters_by_name)

            try:
                shared_publication = await publish_strict_shared_v3_layout(
                    schema_name=resolve_ptg2_schema(),
                    manifest_stage_table=ptg2_manifest_stage_table,
                    reserved_snapshot_key=shared_layout_reservation.snapshot_key,
                    build_token=shared_layout_build_token,
                    expected_coverage_scope_id=(
                        shared_input_identity.coverage_scope_id
                    ),
                    logical_snapshot_id=snapshot_id,
                    expected_source_identities=(
                        shared_input_identity.source_identities
                    ),
                    serving_run_entries=run_entries,
                    code_dictionary_entries=code_dictionary_entries,
                    provider_set_metadata_entries=provider_set_metadata_entries,
                    source_audit_witness_entries=source_audit_witness_entries,
                    price_set_summary_source_count=int(
                        (
                            manifest_merge_metrics_by_name.get("source_files_by_kind")
                            or {}
                        ).get("price_set_summary")
                        or 0
                    ),
                    expected_raw_source_sha256=tuple(
                        str(pair.get("raw_container_sha256") or "")
                        for pair in source_identity_traces
                    ),
                    graph_artifact_entries=list(
                        manifest_artifacts.get("sidecars") or []
                    ),
                    tax_identity_source_artifacts=tax_identity_source_artifacts,
                    provider_identifier_quarantine=provider_identifier_quarantine,
                    compressed_acquisition_entries=(
                        tuple(
                            {
                                "raw_sha256": downloaded.raw_artifact.raw_sha256,
                                "byte_count": downloaded.raw_artifact.byte_count,
                            }
                            for downloaded in buffered_downloads
                            if downloaded.raw_artifact is not None
                        )
                        if provider_graph_v4_enabled
                        else None
                    ),
                    scratch_parent=ptg2_temp_parent(),
                    provider_graph_v4=provider_graph_v4_enabled,
                    **(
                        {"progress_callback": (report_snapshot_publication_progress)}
                        if provider_graph_v4_enabled
                        else {}
                    ),
                    **(
                        {
                            "empty_npi_tin_only_normalization_count": (
                                empty_npi_tin_only_normalization_count
                            )
                        }
                        if provider_graph_v4_enabled
                        else {}
                    ),
                    **(
                        {"full_rebuild_scope_digest": rebuild_scope_digest}
                        if rebuild_scope_digest is not None
                        else {}
                    ),
                )
            finally:
                _cleanup_manifest_copy_entries(strict_v3_copy_entries)
                _cleanup_strict_v3_graph_artifacts(manifest_artifacts)
                pending_strict_v3.copy_entries_by_kind = {}
                pending_strict_v3.graph_artifacts_map = {}
            if (
                rebuild_scope_digest is not None
                and shared_publication.layout_reused_at_seal
            ):
                failure_report_by_field["shared_layout_reused_at_seal"] = True
                _assert_full_rebuild_is_fresh(
                    _full_rebuild_proof_metrics(
                        full_rebuild_stage_tracker.snapshot(),
                        full_rebuild_scope_digest=rebuild_scope_digest,
                        shared_layout_reused=False,
                        shared_layout_reused_at_seal=True,
                        finalizer_block_copy=(
                            shared_publication.serving_index.get("finalizer_block_copy")
                        ),
                    )
                )
            serving_index = {
                **dict(shared_publication.serving_index),
                "source_key": source_key_val,
                "coverage_scope_id": shared_input_identity.coverage_scope_hex,
                "source_set": source_set,
                "provider_identifier_quarantine": provider_identifier_quarantine,
                "source_trace_set_hash": manifest_artifacts.get(
                    "source_trace_set_hash"
                ),
                "network_names": list(manifest_artifacts.get("network_names") or []),
            }
            manifest_merge_metrics_by_name["serving_rows"] = serving_index.get(
                "serving_rates"
            )
            failure_report_by_field.update(
                {
                    "shared_snapshot_key": shared_publication.snapshot_key,
                    "shared_layout_reused_at_seal": (
                        shared_publication.layout_reused_at_seal
                    ),
                    "shared_stored_byte_count": shared_publication.stored_byte_count,
                }
            )
            full_rebuild_metrics = _full_rebuild_proof_metrics(
                full_rebuild_stage_tracker.snapshot(),
                full_rebuild_scope_digest=rebuild_scope_digest,
                shared_layout_reused=shared_layout_reservation.reused,
                shared_layout_reused_at_seal=(shared_publication.layout_reused_at_seal),
                finalizer_block_copy=serving_index.get("finalizer_block_copy"),
            )
            failure_report_by_field.update(full_rebuild_metrics)
            failure_report_by_field["serving_index"] = serving_index
            _emit_ptg2_publish_progress(
                "snapshot tables published",
                completed_steps=6,
                total_steps=publish_progress_total,
                message_text="PTG manifest snapshot tables published",
                serving_rates=(
                    serving_index.get("serving_rates")
                    if isinstance(serving_index, dict)
                    else None
                ),
                rate_count=(
                    serving_index.get("rate_count")
                    if isinstance(serving_index, dict)
                    else None
                ),
                **(
                    {
                        "stage_id": "ptg2_v4_publication",
                        "stage_ordinal": 6,
                    }
                    if provider_graph_v4_enabled
                    else {}
                ),
            )
        publish_seconds = _ptg2_monotonic() - publish_started_monotonic
        post_publish_started_monotonic = _ptg2_monotonic()
        post_publish_seconds_by_stage: dict[str, float] = {}
        post_publish_stage_timer = _StageTimer(
            post_publish_seconds_by_stage,
            post_publish_started_monotonic,
        )

        validated_at = _utcnow()
        serving_timings = (
            serving_index.get("timings", {}) if isinstance(serving_index, dict) else {}
        )
        setup_seconds = data_started_monotonic - import_started_monotonic
        timing_by_metric = {
            "setup_seconds": setup_seconds,
            "data_seconds": data_seconds,
            "publish_seconds": publish_seconds,
            "manifest_precopy_merge_seconds": manifest_precopy_merge_seconds,
        }
        for key, stage_seconds in setup_seconds_by_stage.items():
            timing_by_metric[f"setup_{key}_seconds"] = stage_seconds
        if isinstance(serving_timings, dict):
            for key, stage_seconds in serving_timings.items():
                try:
                    timing_key = f"serving_{key}" if key in timing_by_metric else key
                    timing_by_metric[timing_key] = float(stage_seconds)
                except (TypeError, ValueError):
                    continue
        report_by_field = {
            **failure_report_by_field,
            "serving_index": serving_index,
            "timings": timing_by_metric,
            "manifest_precopy_merge": manifest_merge_metrics_by_name,
            "data_domains": [
                PTG2_DOMAIN_IN_NETWORK,
                *([PTG2_DOMAIN_ALLOWED_AMOUNT] if successful_allowed_files else []),
            ],
        }
        if successful_allowed_files:
            report_by_field.update(
                {
                    "allowed_amount_index": (
                        _allowed_amount_index_manifest(
                            allowed_metrics_by_name,
                            source_key=source_key_val,
                            previous_snapshot_id=(previous_allowed_snapshot_id),
                        )
                    ),
                    **allowed_metrics_by_name,
                }
            )
        if isinstance(serving_index, dict):
            authoritative_rate_count = serving_index.get(
                "serving_rates", serving_index.get("rate_count")
            )
            if authoritative_rate_count is not None:
                report_by_field["serving_rates"] = int(authoritative_rate_count)
                report_by_field["rate_count"] = int(authoritative_rate_count)
        snapshot_publish_by_field = {
            "snapshot_id": snapshot_id,
            "import_run_id": import_run_id,
            "import_month": import_month_value,
            "status": PTG2_STATUS_VALIDATED,
            "created_at": now,
            "validated_at": validated_at,
            "published_at": None,
            "previous_snapshot_id": previous_snapshot_id,
            "manifest": {
                **report_by_field,
                "timings": dict(timing_by_metric),
            },
        }
        if (
            not isinstance(serving_index, dict)
            or serving_index.get("shared_snapshot_key") is None
        ):
            raise RuntimeError("strict V3 publish did not return a shared snapshot key")
        published_shared_snapshot_key = int(serving_index["shared_snapshot_key"])
        _emit_ptg2_publish_progress(
            "staging validated candidate",
            completed_steps=6,
            total_steps=publish_progress_total,
            message_text="binding validated PTG candidate without changing live pointers",
        )
        candidate_result = await _stage_ptg2_source_candidate(
            source_key=source_key_val,
            snapshot_id=snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            import_month=import_month_value,
            updated_at=validated_at,
            snapshot_attributes=snapshot_publish_by_field,
            shared_snapshot_key=published_shared_snapshot_key,
            coverage_scope_id=shared_input_identity.coverage_scope_id,
            coverage_plan_scopes=shared_input_identity.logical_plans,
        )
        candidate_stage_flags_by_name["staged"] = True
        candidate_attributes_by_field = dict(candidate_result["candidate_attributes"])
        if should_auto_activate_candidates:
            activated_at = _utcnow()
            await _publish_ptg2_source_pointers(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                import_month=import_month_value,
                updated_at=activated_at,
                snapshot_attributes=activated_snapshot_attributes(
                    candidate_attributes_by_field,
                    activated_at=activated_at,
                    activation_mode="automatic",
                ),
            )
            is_current_pointer_published = True
            if successful_allowed_files:
                report_by_field["allowed_amount_pointer"] = (
                    await _publish_allowed_current_pointer(
                        source_key=source_key_val,
                        snapshot_id=snapshot_id,
                        previous_snapshot_id=previous_allowed_snapshot_id,
                        import_month=import_month_value,
                        updated_at=activated_at,
                    )
                )
                allowed_snapshot_state_by_name["published"] = True
            activation_status = "activated"
            snapshot_status = PTG2_STATUS_PUBLISHED
        else:
            activation_status = "deferred"
            snapshot_status = PTG2_STATUS_VALIDATED
        release_current_artifact_lease()
        post_publish_stage_timer.mark("logical_candidate_and_optional_pointer_cutover")
        _emit_ptg2_publish_progress(
            "cleaning old source tables",
            completed_steps=7,
            total_steps=publish_progress_total,
            message_text="cleaning old PTG source tables",
        )
        if should_auto_activate_candidates:
            await _cleanup_old_ptg2_source_tables(
                source_key_val,
                {snapshot_id},
                lock_pointer_state=True,
            )
        post_publish_stage_timer.mark("old_state_cleanup")
        _emit_ptg2_publish_progress(
            "address refresh",
            completed_steps=7,
            total_steps=publish_progress_total,
            message_text="checking PTG address-refresh follow-up",
        )
        address_refresh_result = (
            await _enqueue_address_refresh_after_import(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
                has_serving_files=True,
                source_scoped_compact=True,
                test_mode=test_mode,
            )
            if should_auto_activate_candidates
            else {"status": "skipped", "reason": "candidate-activation-deferred"}
        )
        post_publish_stage_timer.mark("address_refresh")
        _cleanup_manifest_copy_entries(pending_strict_v3.copy_entries_by_kind)
        _cleanup_strict_v3_graph_artifacts(pending_strict_v3.graph_artifacts_map)
        pending_strict_v3.copy_entries_by_kind = {}
        pending_strict_v3.graph_artifacts_map = {}
        post_publish_stage_timer.mark("scratch_cleanup")
        _emit_ptg2_publish_progress(
            "persisting completion",
            completed_steps=7,
            total_steps=publish_progress_total,
            message_text="persisting final PTG import state",
            address_refresh_status=(
                address_refresh_result.get("status")
                if isinstance(address_refresh_result, dict)
                else None
            ),
        )
        report_by_field["address_refresh"] = address_refresh_result
        report_by_field["activation_status"] = activation_status
        await _persist_completed_ptg2_import_run(
            _CompletedImportPersistence(
                import_run_id=import_run_id,
                snapshot_id=snapshot_id,
                manifest_stage_table=ptg2_manifest_stage_table,
                import_month=import_month_value,
                started_at=now,
                options=options_by_name,
                report_payload=report_by_field,
                timing_payload=timing_by_metric,
                import_started_monotonic=import_started_monotonic,
                post_publish_started_monotonic=post_publish_started_monotonic,
                post_publish_stage_timer=post_publish_stage_timer,
            )
        )
        ptg2_manifest_stage_table = None
        _emit_ptg2_publish_progress(
            "validated",
            completed_steps=8,
            total_steps=publish_progress_total,
            message_text="PTG publish validation complete",
            address_refresh_status=(
                address_refresh_result.get("status")
                if isinstance(address_refresh_result, dict)
                else None
            ),
        )
        done_line = (
            "PTG2_IMPORT_DONE"
            f"\timport_run_id={import_run_id}"
            f"\tsnapshot_id={snapshot_id}"
            f"\tstatus={snapshot_status}"
            f"\tactivation_status={activation_status}"
            f"\tfiles_processed={processed_file_count_map['done']}"
            f"\tfiles_failed={len(failed_files)}"
            f"\tserving_rates={report_by_field.get('serving_rates', 'unknown')}"
            f"\ttotal_seconds={timing_by_metric['total_seconds']:.2f}"
            f"\tsetup_seconds={timing_by_metric['setup_seconds']:.2f}"
            f"\tdata_seconds={timing_by_metric['data_seconds']:.2f}"
            f"\tpublish_seconds={timing_by_metric['publish_seconds']:.2f}"
            f"\tpost_publish_seconds={timing_by_metric['post_publish_seconds']:.2f}"
            f"\tindex_seconds={float(timing_by_metric.get('index_seconds', 0.0)):.2f}"
            f"\tanalyze_seconds={float(timing_by_metric.get('analyze_seconds', 0.0)):.2f}"
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
            message=(
                "PTG import succeeded"
                if should_auto_activate_candidates
                else "PTG candidate validated; live pointers unchanged"
            ),
        )
        return {
            "status": "succeeded",
            "arch_version": "postgres_binary_v3",
            "storage_generation": shared_storage_generation,
            "activation_status": activation_status,
            "snapshot_status": snapshot_status,
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
            "serving_rates": report_by_field.get("serving_rates"),
            "rate_count": report_by_field.get("rate_count"),
            "source_file_versions": _source_file_versions_from_results(
                successful_files + skipped_files
            ),
            **_frozen_manifest_result_fields(report_by_field),
            "address_refresh": address_refresh_result,
            **allowed_metrics_by_name,
            **full_rebuild_metrics,
            "timings": timing_by_metric,
        }
    except StaleMetadataFenceError:
        write_live_progress(
            status="failed",
            phase="fenced",
            eta_seconds=0,
            message="PTG import stopped: attempt was reconciled",
        )
        raise
    except asyncio.CancelledError as exc:
        await mark_import_failed(
            "worker task was cancelled",
            progress_message="PTG import interrupted: worker task was cancelled",
        )
        _attach_full_rebuild_failure_metrics(exc, failure_report_by_field)
        raise
    except PTG2ArtifactStageFreshnessError as exc:
        freshness_error = PTG2FullRebuildFreshnessError(
            "controlled full rebuild repeated an artifact stage; "
            "create a new attempt after correcting the dataflow",
            _full_rebuild_proof_metrics(
                full_rebuild_stage_tracker.snapshot(),
                full_rebuild_scope_digest=rebuild_scope_digest,
                shared_layout_reused=bool(
                    failure_report_by_field.get("shared_layout_reused")
                ),
                shared_layout_reused_at_seal=bool(
                    failure_report_by_field.get("shared_layout_reused_at_seal")
                ),
            ),
        )
        await mark_import_failed(freshness_error)
        _attach_full_rebuild_failure_metrics(
            freshness_error,
            failure_report_by_field,
        )
        raise freshness_error from exc
    except Exception as exc:
        if is_stale_metadata_fence_error(exc):
            write_live_progress(
                status="failed",
                phase="fenced",
                eta_seconds=0,
                message="PTG import stopped: attempt was reconciled",
            )
            raise_stale_metadata_fence(exc)
        await mark_import_failed(exc)
        _attach_full_rebuild_failure_metrics(exc, failure_report_by_field)
        raise
    finally:
        try:
            await _stop_ptg2_import_heartbeat(ptg2_import_heartbeat_task)
        finally:
            try:
                _cleanup_manifest_copy_entries(pending_strict_v3.copy_entries_by_kind)
                _cleanup_strict_v3_graph_artifacts(
                    pending_strict_v3.graph_artifacts_map
                )
            finally:
                try:
                    await source_import_lock.__aexit__(None, None, None)
                except Exception:
                    logger.warning(
                        "Failed to release PTG2 source import lock",
                        exc_info=True,
                    )
                finally:
                    reset_live_progress_context(live_token)


def _forwarded_main_arguments(
    explicit_arguments_by_name: dict[str, Any],
) -> dict[str, Any]:
    runtime_options_by_name = dict(
        explicit_arguments_by_name.pop("runtime_options_by_name", {})
    )
    allowed_runtime_options = {
        "control_run_id",
        "control_attempt_id",
        "control_attempt_started_at",
        "full_rebuild_scope_digest",
    }
    unsupported_options = sorted(set(runtime_options_by_name) - allowed_runtime_options)
    if unsupported_options:
        unsupported = ", ".join(unsupported_options)
        raise TypeError(f"main() got unexpected keyword argument(s): {unsupported}")
    return {
        **explicit_arguments_by_name,
        **runtime_options_by_name,
    }


async def run_ptg_command(
    test_mode: bool = False,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    source_file_import_id: str | None = None,
    frozen_rate_file_set_contract: str | None = None,
    frozen_rate_files: list[dict[str, Any]] | None = None,
    frozen_rate_file_set_sha256: str | None = None,
    frozen_rate_file_count: int | None = None,
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
    control_attempt_id: str | None = None,
    control_attempt_started_at: str | None = None,
    full_rebuild_scope_digest: str | None = None,
    **runtime_options_by_name: Any,
) -> dict[str, Any]:
    """Run one PTG import while retaining shared inputs through a live lease."""

    return await _guard_ptg_main_artifact_lease(_forwarded_main_arguments(locals()))


async def _guard_ptg_main_artifact_lease(
    forwarded_arguments: dict[str, Any],
) -> dict[str, Any]:
    """Run the forwarded import arguments under one retained-artifact lease."""

    if forwarded_arguments.get("full_rebuild_scope_digest") is None:
        forwarded_arguments.pop("full_rebuild_scope_digest", None)
    lease_owner = str(
        forwarded_arguments.get("control_run_id")
        or forwarded_arguments.get("import_id")
        or forwarded_arguments.get("source_key")
        or f"standalone-{uuid.uuid4().hex}"
    )
    with artifact_lease_context(owner=f"ptg:{lease_owner}") as lease:
        return await guard_artifact_lease(
            lease,
            _main_with_artifact_lease(**forwarded_arguments),
        )


main = run_ptg_command
main.__name__ = "main"


def _default_ptg2_import_id(
    import_month_value: datetime.date,
    source_key_val: str | None,
    *,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    provider_ref_url: str | None = None,
    arch_variant: str | None = None,
) -> str:
    month_id = import_month_value.strftime("%Y%m%d")
    if not source_key_val:
        return month_id
    source_inputs_by_name = {
        "source_key": source_key_val,
        "toc_urls": toc_urls or [],
        "toc_list": toc_list or "",
        "in_network_url": in_network_url or "",
        "allowed_url": allowed_url or "",
        "provider_ref_url": provider_ref_url or "",
        "arch_variant": arch_variant or "",
    }
    if not any(
        source_inputs_by_name[key]
        for key in (
            "toc_urls",
            "toc_list",
            "in_network_url",
            "allowed_url",
            "provider_ref_url",
        )
    ):
        return month_id
    fingerprint = hash_prefix(
        semantic_hash(
            {"import_month": month_id, **source_inputs_by_name},
            domain="ptg2_import_identity",
        ),
        16,
    )
    return f"{month_id}_{fingerprint}"


def _frozen_ptg2_import_id(
    import_month_value: datetime.date,
    source_key_val: str | None,
    *,
    frozen_rate_file_set_sha256: str,
    frozen_rate_file_count: int,
    arch_variant: str,
) -> str:
    """Bind the default import identity to the complete frozen multipart set."""

    month_id = import_month_value.strftime("%Y%m%d")
    if not source_key_val:
        return month_id
    fingerprint = hash_prefix(
        semantic_hash(
            {
                "import_month": month_id,
                "source_key": source_key_val,
                "frozen_rate_file_set_sha256": (frozen_rate_file_set_sha256),
                "frozen_rate_file_count": frozen_rate_file_count,
                "arch_variant": arch_variant,
            },
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
    "build_rate_pack",
    "build_rate_pack_group",
    "build_rate_pack_procedure_group",
    "build_rate_set",
    "build_source_trace_set",
    "canonical_json_dumps",
    "canonicalize_url",
    "choose_reusable_raw_artifact",
    "content_addressed_path",
    "download_raw_artifact",
    "ensure_ptg2_tables",
    "fetch_head_metadata",
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
        return [
            dict(sidecar)
            for sidecar in raw_sidecars.values()
            if isinstance(sidecar, dict)
        ]
    if isinstance(raw_sidecars, list):
        return [dict(sidecar) for sidecar in raw_sidecars if isinstance(sidecar, dict)]
    return []


def _manifest_source_shard_id(
    file_summary: Mapping[str, Any],
    summary_payload: Mapping[str, Any],
    file_index: int,
) -> str:
    file_id = file_summary.get("file_id")
    if file_id is not None:
        return f"file:{file_id}"
    fallback_shard_id = (
        summary_payload.get("logical_sha256")
        or summary_payload.get("raw_sha256")
        or summary_payload.get("engine_source_identity_hash")
        or file_index
    )
    return f"manifest:{fallback_shard_id}"


@dataclass
class _ManifestArtifactCollection:
    sidecar_entries: list[dict[str, Any]] = field(default_factory=list)
    source_trace_hashes: set[str] = field(default_factory=set)
    fallback_trace_set_hashes: set[str] = field(default_factory=set)
    network_names: set[str] = field(default_factory=set)

    def add_manifest_identity(self, manifest_payload: Mapping[str, Any]) -> None:
        """Accumulate source traces and network names from one manifest."""

        source_trace_hash = str(manifest_payload.get("source_trace_hash") or "").strip()
        if source_trace_hash:
            self.source_trace_hashes.add(source_trace_hash)
        else:
            fallback_hash = str(
                manifest_payload.get("source_trace_set_hash") or ""
            ).strip()
            if fallback_hash:
                self.fallback_trace_set_hashes.add(fallback_hash)
        self.network_names.update(
            _normalize_source_network_names(manifest_payload.get("network_names") or [])
        )

    def finish(self) -> dict[str, Any]:
        """Return the canonical aggregate artifact identity."""

        artifacts_by_field: dict[str, Any] = {}
        if self.sidecar_entries:
            artifacts_by_field["sidecars"] = self.sidecar_entries
        if self.source_trace_hashes:
            artifacts_by_field["source_trace_set_hash"] = build_source_trace_set(
                sorted(self.source_trace_hashes)
            )["source_trace_set_hash"]
        elif len(self.fallback_trace_set_hashes) == 1:
            artifacts_by_field["source_trace_set_hash"] = next(
                iter(self.fallback_trace_set_hashes)
            )
        if self.network_names:
            artifacts_by_field["network_names"] = sorted(
                self.network_names,
                key=str.casefold,
            )
        return artifacts_by_field


def _bound_manifest_sidecars(
    file_summary: Mapping[str, Any],
    summary_payload: Mapping[str, Any],
    manifest_payload: dict[str, Any],
    file_index: int,
) -> list[dict[str, Any]]:
    source_shard_id = _manifest_source_shard_id(
        file_summary,
        summary_payload,
        file_index,
    )
    bound_sidecars = _manifest_sidecars_list(manifest_payload)
    if not bound_sidecars:
        raw_path_map = manifest_payload.get("sidecar_paths")
        if not isinstance(raw_path_map, dict):
            return []
        path_by_name = {
            str(name): Path(str(raw_path)) if raw_path else None
            for name, raw_path in raw_path_map.items()
        }
        fallback_sidecars = _collect_ptg2_manifest_sidecar_artifacts(
            path_by_name,
            membership_graph_metrics=manifest_payload.get("membership_graph"),
        )
        bound_sidecars = [dict(sidecar) for sidecar in fallback_sidecars.values()]
    for sidecar in bound_sidecars:
        sidecar["source_shard_id"] = source_shard_id
    _bind_npi_scope_to_source_shard(
        bound_sidecars,
        source_shard_id=source_shard_id,
    )
    return bound_sidecars


def _collect_manifest_artifacts(
    successful_files: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate manifest sidecars, trace identity, and network names by source shard."""

    collection = _ManifestArtifactCollection()
    for file_index, file_summary in enumerate(successful_files):
        summary_payload = (
            file_summary.get("summary") if isinstance(file_summary, dict) else None
        )
        if not isinstance(summary_payload, dict):
            continue
        manifest_payload = summary_payload.get("manifest")
        if not isinstance(manifest_payload, dict):
            continue
        collection.add_manifest_identity(manifest_payload)
        collection.sidecar_entries.extend(
            _bound_manifest_sidecars(
                file_summary,
                summary_payload,
                manifest_payload,
                file_index,
            )
        )
    return collection.finish()


def _bound_tax_identity_source_artifacts(
    file_results: Iterable[Mapping[str, Any]],
    source_assignments: Iterable[Any],
) -> tuple[dict[str, Any], ...]:
    """Bind one authenticated tax sidecar to each dense physical source."""

    binding_index = build_tax_source_bindings(source_assignments)
    sidecar_sources: list[tuple[dict[str, Any], object]] = []
    for file_index, file_summary in enumerate(file_results):
        summary_payload = (
            file_summary.get("summary")
            if isinstance(file_summary, Mapping)
            else None
        )
        manifest_payload = (
            summary_payload.get("manifest")
            if isinstance(summary_payload, Mapping)
            else None
        )
        if not isinstance(manifest_payload, dict):
            continue
        physical_identity = manifest_payload.get("physical_artifact_identity")
        tax_sidecars = tuple(
            sidecar
            for sidecar in _bound_manifest_sidecars(
                file_summary,
                summary_payload,
                manifest_payload,
                file_index,
            )
            if sidecar.get("name") == "provider_group_tax_identity"
        )
        if tax_sidecars and not isinstance(physical_identity, Mapping):
            raise RuntimeError("PTG V4 tax identity source binding is incomplete")
        sidecar_sources.extend(
            (sidecar, physical_identity) for sidecar in tax_sidecars
        )
    return bind_tax_source_sidecars(
        sidecar_sources,
        binding_index=binding_index,
    )
