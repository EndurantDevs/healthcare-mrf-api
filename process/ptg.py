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
import subprocess
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import ijson
from sqlalchemy import cast, func, literal
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import db
from db.models import (ImportLog, PTG2CurrentPlanSource,
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
from process.ptg_parts.ptg2_artifact_blobs import delete_ptg2_artifacts_for_snapshot
from process.ptg_parts.canonical import (_canonical_key, _canonical_sort_key,
                                         _canonicalize_for_json,
                                         canonical_json_dumps,
                                         canonicalize_url, hash_prefix,
                                         normalize_date,
                                         normalize_import_month,
                                         normalize_money,
                                         normalize_tic_source_url,
                                         semantic_hash, sha256_bytes)
from process.ptg_parts.config import (
    PTG2_COPY_UPSERT_ROWS_ENV, PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS,
    PTG2_DEFAULT_RUST_EVENT_QUEUE, PTG2_DEFAULT_RUST_WORKERS,
    PTG2_DIRECT_COPY_SERVING_RATE_ENV, PTG2_DOWNLOAD_RETRIES_ENV,
    PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV, PTG2_DOWNLOAD_TASKS_ENV,
    PTG2_FAST_PROVIDER_UNION_ENV, PTG2_FILE_PROCESS_CONCURRENCY_ENV,
    PTG2_KEEP_PARTIAL_ENV, PTG2_MANIFEST_DIRECT_COPY_TASKS_ENV,
    PTG2_PROVIDER_BUCKET_COUNT_ENV, PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV,
    PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, PTG2_RANGE_DOWNLOAD_TASKS_ENV,
    PTG2_RANGE_DOWNLOADS_ENV, PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV, PTG2_RUST_WORKERS_ENV,
    PTG2_SOURCE_IMPORT_LOCK_ENABLED_ENV, PTG2_STREAMING_DEDUPE_ENV,
    TEST_TOC_FILES, TEST_TOC_JOBS, _env_bool, _env_int,
    _is_postgres_binary_v3_arch, _ptg2_snapshot_arch_from_env)
from process.ptg_parts.config import _ptg2_auto_activate_candidates
from process.ptg_parts.copy_load import (_copy_ignore_ptg2_objects,
                                         _copy_insert_ptg2_objects,
                                         _copy_upsert_ptg2_objects)
from process.ptg_parts.db_tables import (_estimated_table_rows,
                                         _exact_table_rows, _quote_ident,
                                         _table_exists, _table_has_rows)
from process.ptg_parts.domain import (PTG2_ARTIFACT_RAW,
                                      PTG2_CANDIDATE_ACTIVATION_CONTRACT,
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
from process.ptg_parts.input_artifact_retention import (
    artifact_lease_context,
    guard_artifact_lease,
    release_current_artifact_lease,
)
from process.ptg_parts.progress import (_artifact_progress_position,
                                        _format_duration,
                                        _maybe_log_artifact_progress,
                                        _scale_stage_progress_pct, _utcnow)
from process.ptg_parts.provider_cache import (
    PTG2InMemoryProviderReferenceCache, PTG2ProviderReferenceCache,
    _normalize_provider_ref, _provider_cache_get, _provider_cache_hashes,
    _provider_cache_put, _provider_combo_cache_get, _provider_combo_cache_key,
    _provider_combo_cache_put)
from process.ptg_parts.provider_references import (
    _load_provider_references_from_file, _process_provider_reference_file)
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT, PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    membership_index_fence_metadata)
from process.ptg_parts.ptg2_manifest_publish import (
    PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    _copy_price_atom_member_file,
    _copy_ptg2_manifest_price_atom_file,
    _create_ptg2_manifest_serving_stage_table,
    _ptg2_manifest_stage_table_name,
    _ptg2_manifest_support_stage_table)
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
from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    normalized_physical_artifact_identity,
    same_downloaded_physical_input,
    shared_logical_artifact_metadata,
    shared_physical_artifact_identity,
    shared_physical_input_identity,
    shared_snapshot_source_assignments,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    delete_unpublished_shared_v3_snapshot_sources,
    publish_shared_v3_snapshot_sources,
    publish_strict_shared_v3_layout,
    validate_reused_shared_v3_snapshot_sources,
)
from process.ptg_parts.row_helpers import (_as_int_list, _as_list,
                                           _coerce_date, _make_checksum,
                                           _normalize_code_component,
                                           _normalize_tin_type,
                                           _normalize_tin_value,
                                           _normalized_npi_list,
                                           _provider_group_hash_prefix,
                                           _provider_group_identity_hash)
from process.ptg_parts.rust_scanner import (
    _aiter_compact_serving_records_rust, _iter_compact_serving_records_rust,
    _iter_top_level_object_bytes_rust, _ptg2_rust_scanner_binary)
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.snapshot_cleanup import (
    _cleanup_old_ptg2_source_tables, _drop_ptg2_snapshot_table_names,
    _drop_ptg2_snapshot_tables_for_manifest,
    _missing_snapshot_serving_resources, _snapshot_manifest_table_names)
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
                                               _acquire_source_pointer_gc_lock,
                                               _stage_ptg2_source_candidate,
                                               activated_snapshot_attributes,
                                               _ptg2_plan_source_key,
                                               _publish_ptg2_source_pointers,
                                               _source_plan_rows)
from process.ptg_parts.source_versions import _record_source_version
from process.ptg_parts.table_setup import (
    PTG2_MODEL_CLASSES, PTG_CONTROL_TABLE_CLASS_NAMES,
    PTG_PROVIDER_REFERENCE_TABLE_CLASS_NAMES, _drop_ptg2_columns,
    _ensure_indexes, _ensure_ptg_dynamic_tables,
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
_ptg2_monotonic = time.monotonic

PTG2_SOURCE_SCOPED_TEST_ENV = "HLTHPRT_PTG2_SOURCE_SCOPED_TEST"
PTG2_AUTO_ADDRESS_REFRESH_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH"
PTG2_AUTO_ADDRESS_REFRESH_TEST_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_TEST"
PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_LIMIT_PER_SOURCE"
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


class PTG2SnapshotInProgressConflict(RuntimeError):
    """Raised when another delivery owns a deterministic snapshot build."""


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


async def _push_ptg2_snapshot_preserving_publication(
    snapshot_attributes: dict[str, Any],
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
    statement = statement.on_conflict_do_update(
        index_elements=["snapshot_id"],
        set_=update_values_by_column,
        where=conflict_where,
    ).returning(*table.c)

    async def execute_snapshot_state_write() -> dict[str, Any]:
        """Store one snapshot state and report whether a building claim won."""

        stored_row = await statement.first()
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

    if not is_snapshot_claim:
        return await execute_snapshot_state_write()
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        return await execute_snapshot_state_write()


async def _push_ptg2_objects(
    object_entries: list[dict[str, Any]],
    cls,
    rewrite: bool = True,
) -> dict[str, Any] | None:
    if object_entries and cls is PTG2Snapshot and rewrite:
        if len(object_entries) != 1:
            raise ValueError("PTG snapshot state writes must contain exactly one row")
        return await _push_ptg2_snapshot_preserving_publication(object_entries[0])
    if object_entries and cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        try:
            await _copy_ignore_ptg2_objects(object_entries, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/ignore fallback for %s: %s", cls.__tablename__, exc)
    if object_entries and cls is PTG2ServingRate and _env_bool(PTG2_DIRECT_COPY_SERVING_RATE_ENV, False):
        try:
            await _copy_insert_ptg2_objects(object_entries, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 direct COPY fallback for %s: %s", cls.__tablename__, exc)
    if object_entries and rewrite and len(object_entries) >= max(_env_int(PTG2_COPY_UPSERT_ROWS_ENV, 250), 1):
        try:
            await _copy_upsert_ptg2_objects(object_entries, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/upsert fallback for %s: %s", cls.__tablename__, exc)
    try:
        await push_objects(object_entries, cls, rewrite=rewrite, use_copy=False)
    except TypeError as exc:
        if "use_copy" not in str(exc):
            raise
        await push_objects(object_entries, cls, rewrite=rewrite)

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
            **membership_index_fence_metadata(artifact_path),
        }
    return artifacts


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
        raise RuntimeError("PTG2 provider membership sidecar builder returned invalid output") from exc


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
        **{detail_key: detail_value for detail_key, detail_value in progress_details.items() if detail_value is not None},
    }
    try:
        write_live_progress(**progress_payload_dict)
    except Exception:
        logger.debug("Failed to write PTG2 publish live progress", exc_info=True)


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
        summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
        manifest_payload = summary_payload.get("manifest") if isinstance(summary_payload, dict) else None
        copy_files = manifest_payload.get("copy_files") if isinstance(manifest_payload, dict) else None
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


def _collect_manifest_copy_entries(
    successful_files: list[dict[str, Any]],
    copy_kinds: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """Collect metadata-bearing deferred files without opening their payloads."""

    entries_by_kind: dict[str, list[dict[str, Any]]] = {
        kind: [] for kind in copy_kinds
    }
    seen_paths_by_kind: dict[str, set[str]] = {kind: set() for kind in copy_kinds}
    for file_summary in successful_files:
        summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
        manifest_payload = summary_payload.get("manifest") if isinstance(summary_payload, dict) else None
        copy_files = manifest_payload.get("copy_files") if isinstance(manifest_payload, dict) else None
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
    copy_tasks = max(
        _env_int(
            PTG2_MANIFEST_DIRECT_COPY_TASKS_ENV,
            PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS,
        ),
        1,
    )
    _emit_ptg2_publish_progress(
        f"copying {kind}",
        completed_steps=completed_steps_before_copy,
        total_steps=total_steps,
        stage_start_pct=92.0,
        stage_end_pct=95.0,
        message_text=f"copying {kind} worker files into {target_table}",
        copy_kind=kind,
        target_table=target_table,
        input_files=len(existing_paths),
        emitted_rows=emitted_rows,
        direct_to_copy=True,
        copy_tasks=min(copy_tasks, max(len(existing_paths), 1)),
    )
    copy_started_at = time.monotonic()
    if copy_tasks <= 1 or len(existing_paths) <= 1:
        for input_path in existing_paths:
            await copy_func(input_path, target_table=target_table)
    else:
        semaphore = asyncio.Semaphore(copy_tasks)

        async def copy_one(input_path: Path) -> None:
            """Run one copy operation under the shared concurrency limit."""
            async with semaphore:
                await copy_func(input_path, target_table=target_table)

        await asyncio.gather(*(copy_one(input_path) for input_path in existing_paths))
    elapsed_seconds = time.monotonic() - copy_started_at
    row_count = int(emitted_rows or 0)
    _emit_ptg2_publish_progress(
        f"copied {kind}",
        completed_steps=completed_steps_before_copy + 1,
        total_steps=total_steps,
        stage_start_pct=92.0,
        stage_end_pct=95.0,
        message_text=f"copied {row_count} {kind} row(s) into {target_table}",
        copy_kind=kind,
        target_table=target_table,
        input_files=len(existing_paths),
        input_bytes=input_bytes,
        input_rows=row_count,
        output_rows=row_count,
        dropped_rows=0,
        direct_to_copy=True,
        copy_tasks=min(copy_tasks, max(len(existing_paths), 1)),
        elapsed_seconds=elapsed_seconds,
    )
    return {
        "kind": kind,
        "input_files": len(existing_paths),
        "input_bytes": input_bytes,
        "input_rows": row_count,
        "output_rows": row_count,
        "dropped_rows": 0,
        "direct_to_copy": True,
        "copy_tasks": min(copy_tasks, max(len(existing_paths), 1)),
        "elapsed_seconds": elapsed_seconds,
        "rows_per_second": row_count / elapsed_seconds if elapsed_seconds > 0 else None,
        "bytes_per_second": input_bytes / elapsed_seconds if elapsed_seconds > 0 else None,
    }


def _cleanup_manifest_copy_paths(copy_files_by_kind: dict[str, list[Path]]) -> None:
    for copy_file_paths in copy_files_by_kind.values():
        for copy_file_path in copy_file_paths:
            base_copy_path = _manifest_copy_base_path(copy_file_path)
            try:
                copy_file_path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 manifest merge file %s", copy_file_path, exc_info=True)
            _cleanup_empty_manifest_copy_siblings(base_copy_path)


def _cleanup_manifest_copy_entries(
    copy_entries_by_kind: Mapping[str, Sequence[Mapping[str, Any]]],
) -> None:
    paths_by_kind = {
        str(kind): [
            Path(str(entry.get("path")))
            for entry in entries
            if entry.get("path")
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
            logger.warning("Refusing to remove PTG graph artifact outside %s: %s", artifact_root, path)
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning("Failed to remove imported PTG graph artifact %s", path, exc_info=True)
            continue
        parent_directories.add(path.parent)
    for directory in sorted(parent_directories, key=lambda value: len(value.parts), reverse=True):
        current = directory
        while current != artifact_root:
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent


async def _cancel_and_wait_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    """Cancel child work and wait until it can no longer use import inputs."""

    remaining = tuple(tasks)
    for task in remaining:
        task.cancel()
    if remaining:
        await asyncio.gather(*remaining, return_exceptions=True)
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
                db.text(
                    "SELECT pg_try_advisory_lock(hashtextextended(:lock_name, 0))"
                ),
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
                db.text(
                    "SELECT pg_advisory_unlock(hashtextextended(:lock_name, 0))"
                ),
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
    for pattern in (f"{copy_path.name}.worker*", f"{copy_path.name}.provider_refs.worker*"):
        for worker_copy_path in copy_path.parent.glob(pattern):
            try:
                if worker_copy_path.is_file() and worker_copy_path.stat().st_size == 0:
                    worker_copy_path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove empty PTG2 manifest worker copy file %s", worker_copy_path, exc_info=True)


def _cleanup_manifest_copy_family(copy_path: Path) -> None:
    for family_path in (copy_path, *copy_path.parent.glob(f"{copy_path.name}*")):
        try:
            if family_path.is_file():
                family_path.unlink(missing_ok=True)
        except Exception:
            logger.debug("Failed to remove PTG2 manifest copy file %s", family_path, exc_info=True)


async def _merge_and_copy_ptg2_manifest_files(
    *,
    successful_files: list[dict[str, Any]],
    manifest_stage_table: str,
) -> dict[str, Any]:
    if _is_postgres_binary_v3_arch(_ptg2_snapshot_arch_from_env()):
        copy_kinds = ("price_atom", "price_set_atom")
        copy_files_by_kind, emitted_rows_by_kind = _collect_manifest_copy_files(
            successful_files,
            list(copy_kinds),
        )
        if not any(copy_files_by_kind.values()):
            return {"enabled": False, "reason": "no_strict_v3_price_copy_files"}
        target_by_kind = {
            "price_atom": _ptg2_manifest_support_stage_table(
                manifest_stage_table,
                "price_atom",
            ),
            "price_set_atom": _ptg2_manifest_support_stage_table(
                manifest_stage_table,
                "price_set_atom",
            ),
        }
        copy_func_by_kind = {
            "price_atom": _copy_ptg2_manifest_price_atom_file,
            "price_set_atom": _copy_price_atom_member_file,
        }
        copy_report_map: dict[str, Any] = {
            "enabled": True,
            "strict_v3_price_only": True,
            "kinds": {},
            "emitted_rows": emitted_rows_by_kind,
        }
        active_kinds = [kind for kind in copy_kinds if copy_files_by_kind[kind]]
        try:
            for completed_steps, kind in enumerate(active_kinds):
                copy_report_map["kinds"][kind] = await _copy_manifest_files_direct_with_progress(
                    kind,
                    target_table=target_by_kind[kind],
                    input_paths=copy_files_by_kind[kind],
                    copy_func=copy_func_by_kind[kind],
                    completed_steps_before_copy=completed_steps,
                    total_steps=max(len(active_kinds), 1),
                    emitted_rows=emitted_rows_by_kind.get(kind),
                )
            copy_report_map["direct_to_copy"] = True
            _emit_screen_line(
                "PTG2_STRICT_V3_PRICE_COPY\t"
                f"{json.dumps(copy_report_map, sort_keys=True)}"
            )
            return copy_report_map
        finally:
            _cleanup_manifest_copy_paths(copy_files_by_kind)


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
) -> dict[str, Any]:
    """Scan one file into strict V3 COPY artifacts and clean incomplete scratch state."""

    if not ptg2_manifest_stage_table:
        raise RuntimeError("PTG imports require manifest serving stage tables")
    if max_items is not None:
        logger.info("Ignoring max_items=%s for manifest-backed Rust PTG import", max_items)

    plan_fields = _derive_plan_fields(meta, plan_info)
    source_network_name_values = _normalize_source_network_names(source_network_names)
    arch_version = _ptg2_snapshot_arch_from_env()
    if not _is_postgres_binary_v3_arch(arch_version):
        raise RuntimeError("only postgres_binary_v3 PTG imports are supported")
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    _source_trace_row, _source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
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
    procedure_hashes: set[str] = set()
    deferred_copy_entries_by_kind: dict[str, list[dict[str, Any]]] = {
        "serving_run": [],
        "serving_code_dictionary": [],
        "price_atom": [],
        "price_set_atom": [],
        "provider_group_member": [],
        "provider_set_metadata": [],
    }
    deferred_copy_file_paths_by_kind: dict[str, set[str]] = {
        kind: set() for kind in deferred_copy_entries_by_kind
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

    def _record_deferred_copy_file_once(kind: str, copy_file: Path, row_count: int) -> int:
        path_key = _copy_file_key(copy_file)
        seen_paths = deferred_copy_file_paths_by_kind.setdefault(kind, set())
        if path_key in seen_paths:
            return 0
        seen_paths.add(path_key)
        deferred_copy_entries_by_kind[kind].append(
            {"path": str(copy_file), "row_count": row_count}
        )
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
    manifest_provider_group_member_copy_path = _new_copy_path("ptg2_manifest_provider_group_member_")
    manifest_provider_set_metadata_copy_path = _new_copy_path(
        "ptg2_v3_provider_set_metadata_"
    )
    v3_serving_run_directory = Path(
        tempfile.mkdtemp(prefix="ptg2-v3-runs-", dir=copy_tmp_dir)
    )
    manifest_artifact_dir = resolve_ptg2_artifact_dir() / "serving" / _ptg2_snapshot_table_token(
        str(plan_fields.get("plan_id") or "plan"),
        snapshot_id,
    )
    manifest_artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_file_token = hashlib.sha256(str(Path(file_path).resolve()).encode("utf-8")).hexdigest()[:16]
    manifest_sidecar_paths_by_kind = {
        "provider_forward": manifest_artifact_dir
        / f"provider_forward_{manifest_file_token}.ptg2sc",
        "provider_inverted": manifest_artifact_dir
        / f"provider_inverted_{manifest_file_token}.ptg2sc",
        "provider_group_npi": manifest_artifact_dir
        / f"provider_group_npi_{manifest_file_token}.ptg2sc",
        "provider_npi_group": manifest_artifact_dir
        / f"provider_npi_group_{manifest_file_token}.ptg2sc",
    }

    def discard_file_scratch() -> None:
        """Remove all file-local COPY, run, and sidecar scratch artifacts."""

        _cleanup_manifest_copy_entries(deferred_copy_entries_by_kind)
        for copy_path in (
            manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path,
            manifest_provider_group_member_copy_path,
            manifest_provider_set_metadata_copy_path,
        ):
            _cleanup_manifest_copy_family(copy_path)
        shutil.rmtree(v3_serving_run_directory, ignore_errors=True)
        shutil.rmtree(manifest_artifact_dir, ignore_errors=True)

    def record_ready_manifest_file(kind: str, copy_row: dict[str, Any]) -> None:
        """Record one nonempty deferred COPY file exactly once for publication."""

        if kind not in {
            "price_atom",
            "price_set_atom",
            "provider_group_member",
            "provider_set_metadata",
        }:
            return
        raw_copy_path = str(copy_row.get("path") or "").strip()
        if not raw_copy_path:
            return
        copy_file = Path(raw_copy_path)
        copied_rows = int(copy_row.get("row_count") or 0)
        if copied_rows <= 0:
            copied_rows = _ptg2_copy_file_row_count(copy_file)
        _record_deferred_copy_file_once(kind, copy_file, copied_rows)

    try:
        async for record_kind, record_row in _aiter_compact_serving_records_rust(
            file_path,
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
            manifest_provider_npi_sidecar_path=None,
            manifest_price_forward_sidecar_path=None,
            manifest_price_atom_copy_path=manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path=manifest_price_set_atom_copy_path,
            manifest_provider_group_member_copy_path=manifest_provider_group_member_copy_path,
            manifest_code_count_copy_path=None,
            manifest_provider_set_dictionary_copy_path=manifest_provider_set_metadata_copy_path,
            source_network_names=source_network_name_values,
            manifest_only=True,
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
            rust_records += 1
            if record_kind == "manifest_price_atom_copy_file":
                record_ready_manifest_file("price_atom", record_row)
            if record_kind == "manifest_price_set_atom_copy_file":
                record_ready_manifest_file("price_set_atom", record_row)
            if record_kind == "manifest_provider_group_member_copy_file":
                record_ready_manifest_file("provider_group_member", record_row)
            if record_kind == "manifest_provider_set_dictionary_copy_file":
                record_ready_manifest_file("provider_set_metadata", record_row)
            if record_kind in {"procedure", "serving_rate_compact"} and record_row.get("procedure_hash"):
                procedure_hashes.add(str(record_row.get("procedure_hash")))
        for copy_path, kind in (
            (manifest_price_atom_copy_path, "price_atom"),
            (manifest_price_set_atom_copy_path, "price_set_atom"),
            (manifest_provider_group_member_copy_path, "provider_group_member"),
            (manifest_provider_set_metadata_copy_path, "provider_set_metadata"),
        ):
            for candidate_copy_path in _manifest_copy_candidates(copy_path):
                if candidate_copy_path.exists() and candidate_copy_path.stat().st_size > 0:
                    record_ready_manifest_file(
                        kind,
                        {"path": str(candidate_copy_path), "row_count": 0},
                    )
        is_scan_complete = True
    finally:
        manifest_copy_paths = (
            manifest_price_atom_copy_path,
            manifest_price_set_atom_copy_path,
            manifest_provider_group_member_copy_path,
            manifest_provider_set_metadata_copy_path,
        )
        for copy_path in manifest_copy_paths:
            try:
                if copy_path.exists() and copy_path.stat().st_size == 0:
                    copy_path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove empty PTG2 manifest copy file %s", copy_path, exc_info=True)
            _cleanup_empty_manifest_copy_siblings(copy_path)
        if not is_scan_complete:
            for copy_path in manifest_copy_paths:
                _cleanup_manifest_copy_family(copy_path)
            discard_file_scratch()

    membership_graph_metrics_map: dict[str, Any] = {}
    provider_npi_scope_copy_path = _new_copy_path("ptg2_manifest_provider_npi_scope_")
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
        manifest_sidecar_paths_by_kind
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
    """Download and filter one table of contents, persist files, and return jobs."""
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
            if entry.domain != PTG2_DOMAIN_IN_NETWORK:
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

    if not toc_content.get("reporting_structure"):
        for catalog_entry in parsed_catalog_entries:
            if _has_reached_toc_file_limit(jobs, max_files):
                break
            if catalog_entry.domain == PTG2_DOMAIN_IN_NETWORK:
                job_type = "in_network"
                file_type = "in-network"
            else:
                continue
            location = normalize_tic_source_url(catalog_entry.original_url)
            if not _is_requested_toc_body_file_url(location, file_url_match_tokens):
                continue
            meta = {
                "reporting_entity_name": catalog_entry.reporting_entity_name,
                "reporting_entity_type": catalog_entry.reporting_entity_type,
            }
            plans = list(catalog_entry.plan_info or ())
            file_row = _build_file_row(
                location,
                file_type,
                meta,
                plans,
                catalog_entry.description,
                catalog_entry.from_index_url or toc_url,
            )
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])
            jobs.append(
                {
                    "type": job_type,
                    "url": location,
                    "description": catalog_entry.description,
                    "plan_info": plans,
                    "from_index_url": catalog_entry.from_index_url or toc_url,
                    "meta": meta,
                }
            )
            if test_mode and len(jobs) >= TEST_TOC_JOBS:
                break

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
    meta = provided_meta or await _extract_metadata_fields(logical_artifact.logical_path)
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


async def _process_in_network_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    snapshot_id: str | None = None,
    coverage_scope_id: str | None = None,
    import_month: datetime.date | None = None,
    ptg2_manifest_stage_table: str | None = None,
    source_network_names: list[str] | str | None = None,
    raw_artifact: PTG2RawArtifact | None = None,
    logical_artifact: PTG2LogicalArtifact | None = None,
    recorded_provenance: Mapping[str, Any] | None = None,
) -> PTG2FileProcessResult:
    """Scan one in-network job into strict V3 staging and return its result."""
    url = job["url"]
    plan_info = job.get("plan_info")
    if not coverage_scope_id or not re.fullmatch(r"[0-9a-f]{64}", coverage_scope_id):
        raise ValueError("strict V3 file processing requires a 32-byte coverage scope id")

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
        provenance_map = dict(recorded_provenance or {})
        if not provenance_map:
            provenance_map = await _record_in_network_file_provenance(
                job,
                classes,
                raw_artifact=raw_artifact,
                logical_artifact=logical_artifact,
                import_run_id=import_run_id,
            )
        source_metadata_map = dict(provenance_map["meta"])
        file_record_map = dict(provenance_map["file_row"])
        source_version = provenance_map["source_version"]
        source_network_name_values = _normalize_source_network_names(source_network_names or job.get("source_network_names"))
        parse_summary = await _parse_in_network_file_strict_v3(
            extracted,
            file_record_map["file_id"],
            source_metadata_map,
            plan_info,
            test_mode,
            import_log_cls,
            url,
            source_version,
            snapshot_id or "ptg2:unknown",
            coverage_scope_id,
            import_month or normalize_import_month(None),
            max_items=max_items,
            ptg2_manifest_stage_table=ptg2_manifest_stage_table,
            source_network_names=source_network_name_values,
        )
    if not test_mode and int((parse_summary or {}).get("serving_rates") or 0) <= 0:
        no_data_summary = dict(parse_summary or {})
        no_data_summary["skipped_reason"] = "parsed zero serving rates"
        no_data_summary.update(_source_version_summary(source_version))
        return PTG2FileProcessResult(
            "in_network",
            url,
            True,
            file_id=file_record_map["file_id"],
            summary=no_data_summary,
            skipped=True,
        )
    summary_payload = dict(parse_summary or {})
    summary_payload.update(_source_version_summary(source_version))
    return PTG2FileProcessResult(
        "in_network",
        url,
        True,
        file_id=file_record_map["file_id"],
        summary=summary_payload,
    )


async def _persist_completed_ptg2_import_run(
    *,
    import_run_id: str,
    import_month: datetime.date,
    started_at: datetime.datetime,
    options: Mapping[str, Any],
    report_payload: dict[str, Any],
    timing_payload: dict[str, Any],
    import_started_monotonic: float,
    post_publish_started_monotonic: float,
    post_publish_stage_timer: _StageTimer,
) -> datetime.datetime:
    """Persist completion only after every required import stage has finished."""

    provisional_finished_at = _utcnow()
    provisional_report = {
        **report_payload,
        "timings": dict(timing_payload),
        "timing_contract": {
            "version": 2,
            "completion_metrics_pending": True,
        },
    }
    await _push_ptg2_objects(
        [
            {
                "import_run_id": import_run_id,
                "import_month": import_month,
                "status": PTG2_STATUS_VALIDATED,
                "started_at": started_at,
                "finished_at": provisional_finished_at,
                "heartbeat_at": provisional_finished_at,
                "options": dict(options),
                "report": provisional_report,
                "error": None,
            }
        ],
        PTG2ImportRun,
        rewrite=True,
    )
    post_publish_stage_timer.mark("run_state_persistence")

    completed_monotonic = _ptg2_monotonic()
    for key, value in post_publish_stage_timer.durations_by_stage.items():
        timing_payload[f"post_publish_{key}_seconds"] = value
    timing_payload["post_publish_seconds"] = (
        completed_monotonic - post_publish_started_monotonic
    )
    timing_payload["total_seconds"] = completed_monotonic - import_started_monotonic
    report_payload["timings"] = timing_payload
    report_payload["timing_contract"] = {
        "version": 2,
        "total_boundary": "after_required_run_state_persistence",
        "completion_metrics_write_excluded": True,
    }

    completed_at = _utcnow()
    await _push_ptg2_objects(
        [
            {
                "import_run_id": import_run_id,
                "import_month": import_month,
                "status": PTG2_STATUS_VALIDATED,
                "started_at": started_at,
                "finished_at": completed_at,
                "heartbeat_at": completed_at,
                "options": dict(options),
                "report": report_payload,
                "error": None,
            }
        ],
        PTG2ImportRun,
        rewrite=True,
    )
    return completed_at


async def _heartbeat_ptg2_import_run(import_run_id: str) -> None:
    """Keep the internal PTG run lease current while a long import is active."""

    interval = max(
        float(os.getenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "15")),
        1.0,
    )
    schema = _quote_ident(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    while True:
        await asyncio.sleep(interval)
        try:
            await db.status(
                f"""
                UPDATE {schema}.ptg2_import_run
                   SET heartbeat_at = timezone('UTC', statement_timestamp())
                 WHERE import_run_id = :import_run_id
                   AND status IN ('pending', 'running', 'building')
                """,
                import_run_id=import_run_id,
            )
        except Exception:
            logger.warning(
                "Failed to persist PTG2 import heartbeat for %s",
                import_run_id,
                exc_info=True,
            )


async def _stop_ptg2_import_heartbeat(task: asyncio.Task[Any] | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


async def _mark_ptg2_import_failed(
    import_run_id: str,
    snapshot_id: str,
    import_month: datetime.date,
    started_at: datetime.datetime,
    error: BaseException | str,
    report: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    *,
    should_preserve_published_snapshot: bool = False,
    import_started_monotonic: float | None = None,
    failure_handling_started_monotonic: float | None = None,
) -> dict[str, Any] | None:
    """Persist failed import state and return its report, or None on persistence failure."""
    finished = _utcnow()
    error_text = str(error)
    report_payload = dict(report or {})
    report_payload.setdefault("snapshot_id", snapshot_id)
    timing_payload = dict(report_payload.get("timings") or {})
    timing_payload.pop("total_seconds", None)
    report_payload["timings"] = timing_payload
    persistence_started_monotonic = _ptg2_monotonic()
    try:
        if not should_preserve_published_snapshot:
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
                        "manifest": {
                            **report_payload,
                            "error": error_text,
                        },
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
                    "report": {
                        **report_payload,
                        "timing_contract": {
                            "version": 2,
                            "completion_metrics_pending": True,
                        },
                    },
                    "error": error_text,
                }
            ],
            PTG2ImportRun,
            rewrite=True,
        )
        persisted_monotonic = _ptg2_monotonic()
        if import_started_monotonic is not None:
            timing_payload["failure_state_persistence_seconds"] = (
                persisted_monotonic - persistence_started_monotonic
            )
            if failure_handling_started_monotonic is not None:
                timing_payload["failure_handling_seconds"] = (
                    persisted_monotonic - failure_handling_started_monotonic
                )
            timing_payload["total_seconds"] = (
                persisted_monotonic - import_started_monotonic
            )
            report_payload["timings"] = timing_payload
            report_payload["timing_contract"] = {
                "version": 2,
                "total_boundary": "after_required_failure_state_persistence",
                "completion_metrics_write_excluded": True,
            }
            finished = _utcnow()
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
        return report_payload
    except Exception as mark_exc:
        logger.error("Failed to mark PTG2 import %s as failed: %s", import_run_id, mark_exc)
        return None


async def _is_failed_shared_layout_abandoned(
    shared_layout_reservation: Any,
    *,
    build_token: str,
) -> bool | None:
    """Abandon an owned unpublished layout, or defer interrupted cleanup to GC."""
    if shared_layout_reservation is None or shared_layout_reservation.reused:
        return None
    for attempt in range(3):
        try:
            async with db.transaction() as session:
                return await is_shared_layout_build_abandoned(
                    session,
                    schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
                    snapshot_key=shared_layout_reservation.snapshot_key,
                    build_token=build_token,
                )
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
    manifest_stage_table: str | None,
    snapshot_id: str,
) -> None:
    """Remove unpublished relational, artifact, and source-dictionary state."""
    try:
        await _drop_ptg2_snapshot_tables_for_manifest(serving_index)
        if manifest_stage_table:
            await _drop_ptg2_snapshot_table_names(
                _ptg2_manifest_stage_table_names(manifest_stage_table)
            )
    except Exception:
        logger.debug(
            "Failed to clean PTG2 source-scoped tables for failed import",
            exc_info=True,
        )
    try:
        await delete_ptg2_artifacts_for_snapshot(snapshot_id)
    except Exception:
        logger.debug("Failed to clean PTG2 artifacts for failed import", exc_info=True)
    try:
        await delete_unpublished_shared_v3_snapshot_sources(
            schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            snapshot_id=snapshot_id,
        )
    except Exception:
        logger.debug(
            "Failed to clean PTG2 shared source metadata for failed import",
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
        "etag": source_version.etag,
        "last_modified": source_version.last_modified,
    }


def _ptg2_source_file_versions_from_results(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    versions: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for file_result in files:
        summary = (
            file_result.get("summary")
            if isinstance(file_result.get("summary"), dict)
            else {}
        )
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


def _ptg2_snapshot_content_options(option_by_name: dict[str, Any]) -> dict[str, Any]:
    content_option_by_name = {
        key: option_by_name.get(key)
        for key in _PTG2_SNAPSHOT_CONTENT_OPTION_KEYS
    }
    content_option_by_name["toc_urls"] = _dedupe_preserve(
        [
            str(value).strip()
            for value in _as_list(option_by_name.get("toc_urls"))
            if str(value).strip()
        ]
    )
    for key in _PTG2_SNAPSHOT_SET_OPTION_KEYS:
        content_option_by_name[key] = sorted(
            set(_normalize_filter_values(option_by_name.get(key)))
        )
    content_option_by_name["source_network_names"] = sorted(
        set(_normalize_source_network_names(option_by_name.get("source_network_names"))),
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
    serving_index = _published_snapshot_manifest(snapshot_attributes).get("serving_index")
    return dict(serving_index) if isinstance(serving_index, dict) else {}


async def _reconcile_already_published_snapshot(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
) -> dict[str, Any]:
    serving_index = _published_snapshot_serving_index(snapshot_attributes)
    if serving_index.get("storage") != "manifest_snapshot":
        return {"status": "not_applicable", "reason": "snapshot has no source-scoped serving tables"}
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
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.transaction() as session:
        await _acquire_source_pointer_gc_lock(session)
        missing_tables, missing_artifacts = await _missing_snapshot_serving_resources(
            schema_name,
            snapshot_id,
            serving_index,
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
            previous_snapshot_id=str(previous_snapshot_id) if previous_snapshot_id else None,
            import_month=import_month,
            updated_at=_utcnow(),
            snapshot_attributes=snapshot_attributes,
        )


async def _resume_validated_candidate(
    *,
    snapshot_attributes: dict[str, Any],
    snapshot_id: str,
    source_key: str,
    import_month: datetime.date,
    auto_activate: bool,
) -> dict[str, Any]:
    """Return or atomically activate an idempotently redelivered candidate."""

    manifest = _published_snapshot_manifest(snapshot_attributes)
    activation = manifest.get("activation")
    if (
        not isinstance(activation, dict)
        or activation.get("contract") != PTG2_CANDIDATE_ACTIVATION_CONTRACT
        or activation.get("state") != "validated"
    ):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} is validated without the strict V3 candidate contract"
        )
    if str(activation.get("source_key") or "") != source_key:
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} candidate source does not match {source_key}"
        )
    serving_index = manifest.get("serving_index")
    if not isinstance(serving_index, dict):
        raise RuntimeError(f"PTG snapshot {snapshot_id} candidate has no serving index")
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    missing_tables, missing_artifacts = await _missing_snapshot_serving_resources(
        schema_name,
        snapshot_id,
        serving_index,
    )
    if missing_tables or missing_artifacts:
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} candidate resources are missing: "
            + ", ".join([*missing_tables, *missing_artifacts])
        )

    previous_snapshot_id = activation.get("expected_previous_snapshot_id")
    pointer_result: dict[str, Any] | None = None
    if auto_activate:
        activated_at = _utcnow()
        pointer_result = await _publish_ptg2_source_pointers(
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=(
                str(previous_snapshot_id) if previous_snapshot_id else None
            ),
            import_month=import_month,
            updated_at=activated_at,
            snapshot_attributes=activated_snapshot_attributes(
                snapshot_attributes,
                activated_at=activated_at,
                activation_mode="automatic_redelivery",
            ),
        )
    rate_count = manifest.get(
        "serving_rates",
        manifest.get(
            "rate_count",
            serving_index.get("serving_rates", serving_index.get("rate_count")),
        ),
    )
    return {
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
    }


_PTG2_MANIFEST_STAGE_SUPPORT_KINDS = (
    "price_atom",
    "price_set_atom",
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
    manifest = _published_snapshot_manifest(snapshot_attributes)
    serving_index = manifest.get("serving_index")
    serving_index = serving_index if isinstance(serving_index, dict) else {}
    rate_count = manifest.get(
        "serving_rates",
        manifest.get("rate_count", serving_index.get("serving_rates", serving_index.get("rate_count"))),
    )
    return {
        "status": "succeeded",
        "publish_status": "already_published",
        "already_published": True,
        "message": "PTG snapshot is already published; serving pointers were reconciled",
        "import_run_id": str(snapshot_attributes.get("import_run_id") or import_run_id),
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "import_month": import_month.isoformat(),
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "address_refresh": manifest.get("address_refresh"),
        "pointer_reconciliation": pointer_reconciliation,
    }


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
        "storage_bytes",
        "timings",
        "audit_sample",
    }
)


def _reused_shared_v3_serving_index(
    layout_manifest: Mapping[str, Any] | None,
    *,
    source_key: str,
    shared_snapshot_key: int,
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
    if str(serving_index.get("arch_version") or "").strip().lower() != "postgres_binary_v3":
        raise RuntimeError("reusable strict V3 layout has an incompatible architecture")
    if (
        str(serving_index.get("storage_generation") or "").strip().lower()
        != PTG2_V3_SHARED_GENERATION
        or str(serving_index.get("cold_lookup_contract") or "").strip().lower()
        != PTG2_V3_COLD_LOOKUP_CONTRACT
        or str(serving_index.get("price_membership_semantics") or "").strip().lower()
        != PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS
        or str(serving_index.get("serving_multiplicity_semantics") or "").strip().lower()
        != PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
        or str(serving_index.get("shared_block_layout") or "").strip().lower()
        != "dense_shared_blocks_v3"
    ):
        raise RuntimeError("reusable strict V3 layout is missing the shared cold-read contract")
    try:
        source_count = int(serving_index.get("source_count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("reusable strict V3 layout is missing source_count") from exc
    if source_count <= 0:
        raise RuntimeError("reusable strict V3 layout has an invalid source_count")
    try:
        serving_index["provider_identifier_quarantine"] = (
            validate_provider_identifier_quarantine(
                serving_index.get("provider_identifier_quarantine")
            )
        )
    except ValueError as exc:
        raise RuntimeError(
            "reusable strict V3 layout has invalid provider identifier quarantine evidence"
        ) from exc
    try:
        code_count = int(serving_index.get("code_count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("reusable strict V3 layout is missing code_count") from exc
    if code_count < 0:
        raise RuntimeError("reusable strict V3 layout has an invalid code_count")
    serving_index.update(
        {
            "source_key": source_key,
            "shared_snapshot_key": int(shared_snapshot_key),
            "storage_generation": PTG2_V3_SHARED_GENERATION,
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
        scanner_summary = scanner.get("summary") if isinstance(scanner, Mapping) else None
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
) -> tuple[Any, ...]:
    assignments, trace_set_rows = shared_snapshot_source_assignments(
        identity_trace_pairs,
        expected_identities=shared_input_identity.source_identities,
    )
    now = _utcnow()
    await _push_ptg2_objects(
        [{**row, "created_at": now} for row in trace_set_rows],
        PTG2SourceTraceSet,
        rewrite=True,
    )
    await publish_shared_v3_snapshot_sources(
        schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        snapshot_id=snapshot_id,
        plan_id=shared_input_identity.logical_plan.plan_id,
        plan_market_type=shared_input_identity.logical_plan.plan_market_type,
        coverage_scope_id=shared_input_identity.coverage_scope_id,
        assignments=assignments,
    )
    return assignments


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
        meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
        plan_info = job.get("plan_info") if isinstance(job.get("plan_info"), list) else None
        if not str(_derive_plan_fields(meta, plan_info).get("plan_id") or "").strip():
            return False
    return True


_shared_v3_preflight_eligible = _is_shared_v3_preflight_eligible


def _shared_v3_scanner_identity() -> dict[str, Any]:
    """Bind reuse to the exact scanner/finalizer executable that defines output."""

    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("strict shared V3 requires the PTG2 Rust scanner binary")
    digest, byte_count = sha256_file(binary)
    source_root = Path(__file__).resolve().parent
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
    )
    publisher_digest = hashlib.sha256()
    publisher_byte_count = 0
    for source_path in publisher_sources:
        source_bytes = source_path.read_bytes()
        relative_name = source_path.relative_to(source_root).as_posix().encode("utf-8")
        publisher_digest.update(len(relative_name).to_bytes(4, "big"))
        publisher_digest.update(relative_name)
        publisher_digest.update(len(source_bytes).to_bytes(8, "big"))
        publisher_digest.update(source_bytes)
        publisher_byte_count += len(source_bytes)
    return {
        "contract_version": 2,
        "scanner_binary_sha256": digest,
        "scanner_binary_bytes": int(byte_count),
        "publisher_source_sha256": publisher_digest.hexdigest(),
        "publisher_source_bytes": publisher_byte_count,
    }


async def _publish_reused_shared_v3_snapshot(
    *,
    downloaded_jobs: Sequence[PTG2DownloadedJob],
    shared_input_identity: Any,
    classes: Mapping[str, type],
    layout_manifest: Mapping[str, Any] | None,
    shared_snapshot_key: int,
    semantic_fingerprint: bytes,
    coverage_scope_id: bytes,
    coverage_plan_id: str,
    coverage_plan_market_type: str,
    snapshot_id: str,
    import_run_id: str,
    source_key: str,
    import_month: datetime.date,
    previous_snapshot_id: str | None,
    started_at: datetime.datetime,
    options: Mapping[str, Any],
    manifest_stage_table: str | None,
    test_mode: bool,
    import_started_monotonic: float,
    candidate_stage_state: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """Publish a logical snapshot binding without rescanning identical content."""

    source_file_versions: list[dict[str, Any]] = []
    observed_plans: set[tuple[str, str]] = set()
    source_trace_hashes: set[str] = set()
    network_names: set[str] = set()
    source_provenance_entries: list[dict[str, Any]] = []
    for downloaded in downloaded_jobs:
        if downloaded.error or downloaded.raw_artifact is None or downloaded.logical_artifact is None:
            raise RuntimeError("reusable strict V3 input contains an incomplete download")
        job = downloaded.job
        if str(job.get("type") or "").strip().lower() != "in_network":
            raise RuntimeError("strict V3 fast reuse currently requires in-network-only inputs")
        provenance = await _record_in_network_file_provenance(
            job,
            classes,
            raw_artifact=downloaded.raw_artifact,
            logical_artifact=downloaded.logical_artifact,
            import_run_id=import_run_id,
        )
        source_metadata_map = dict(provenance["meta"])
        plan_info = job.get("plan_info") if isinstance(job.get("plan_info"), list) else None
        plan_fields = _derive_plan_fields(source_metadata_map, plan_info)
        plan_id = str(plan_fields.get("plan_id") or "").strip()
        if not plan_id:
            raise RuntimeError("strict V3 fast reuse requires source plan identity metadata")
        plan_market_type = str(plan_fields.get("plan_market_type") or "").strip().lower()
        if (plan_id, plan_market_type) != (
            str(coverage_plan_id).strip(),
            str(coverage_plan_market_type).strip().lower(),
        ):
            raise RuntimeError(
                "strict V3 reusable input plan metadata changed after physical identity planning"
            )
        if (plan_id, plan_market_type) not in observed_plans:
            plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(
                plan_fields,
                snapshot_id,
                import_month,
            )
            await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
            if alias_rows:
                await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
            await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)
            observed_plans.add((plan_id, plan_market_type))
        file_row = provenance["file_row"]
        source_version = provenance["source_version"]
        source_trace_hashes.add(str(provenance["source_trace_hash"]))
        source_provenance_entries.append(
            {
                **shared_physical_artifact_identity(downloaded).as_dict(),
                **shared_logical_artifact_metadata(downloaded),
                "source_trace_hash": str(provenance["source_trace_hash"]),
            }
        )
        network_names.update(provenance["network_names"])
        source_file_versions.append(
            {
                "source_type": "in_network",
                "url": str(job.get("url") or ""),
                "file_id": file_row.get("file_id"),
                **_source_version_summary(source_version),
            }
        )

    serving_index = _reused_shared_v3_serving_index(
        layout_manifest,
        source_key=source_key,
        shared_snapshot_key=shared_snapshot_key,
    )
    serving_index["coverage_scope_id"] = bytes(coverage_scope_id).hex()
    serving_index["source_trace_set_hash"] = build_source_trace_set(
        sorted(source_trace_hashes)
    )["source_trace_set_hash"]
    serving_index["network_names"] = sorted(network_names, key=str.casefold)
    if int(serving_index["source_count"]) != int(shared_input_identity.source_count):
        raise RuntimeError(
            "reusable strict V3 layout source_count does not match the complete physical input"
        )
    source_set = _shared_v3_source_set_metadata(
        source_provenance_entries,
        expected_source_count=shared_input_identity.source_count,
    )
    await _publish_shared_v3_source_dictionary(
        shared_input_identity=shared_input_identity,
        identity_trace_pairs=source_provenance_entries,
        snapshot_id=snapshot_id,
    )
    serving_index["source_set"] = source_set
    await validate_reused_shared_v3_snapshot_sources(
        schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        snapshot_key=int(shared_snapshot_key),
        logical_snapshot_id=snapshot_id,
    )
    post_publish_started_monotonic = _ptg2_monotonic()
    post_publish_stage_timings: dict[str, float] = {}
    post_publish_stage_timer = _StageTimer(
        post_publish_stage_timings,
        post_publish_started_monotonic,
    )
    validated_at = _utcnow()
    rate_count = int(serving_index.get("serving_rates", serving_index.get("rate_count")) or 0)
    timings_by_phase = {
        "data_seconds": 0.0,
        "publish_seconds": 0.0,
        "shared_layout_reuse_seconds": (
            post_publish_started_monotonic - import_started_monotonic
        ),
    }
    publish_report_map = {
        "snapshot_id": snapshot_id,
        "serving_index": serving_index,
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "source_file_versions": source_file_versions,
        "shared_layout_reused": True,
        "shared_snapshot_key": int(shared_snapshot_key),
        "shared_semantic_fingerprint": bytes(semantic_fingerprint).hex(),
        "coverage_scope_id": bytes(coverage_scope_id).hex(),
        "timings": timings_by_phase,
    }
    snapshot_values_by_field = {
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "import_month": import_month,
        "status": PTG2_STATUS_VALIDATED,
        "created_at": started_at,
        "validated_at": validated_at,
        "published_at": None,
        "previous_snapshot_id": previous_snapshot_id,
        "manifest": {
            **publish_report_map,
            "timings": dict(timings_by_phase),
        },
    }
    candidate_result = await _stage_ptg2_source_candidate(
        source_key=source_key,
        snapshot_id=snapshot_id,
        previous_snapshot_id=previous_snapshot_id,
        import_month=import_month,
        updated_at=validated_at,
        snapshot_attributes=snapshot_values_by_field,
        shared_snapshot_key=int(shared_snapshot_key),
        coverage_scope_id=bytes(coverage_scope_id),
        coverage_plan_id=coverage_plan_id,
        coverage_plan_market_type=coverage_plan_market_type,
    )
    if candidate_stage_state is not None:
        candidate_stage_state["staged"] = True
    candidate_attributes = dict(candidate_result["candidate_attributes"])
    auto_activate = bool(options.get("auto_activate_candidates", False))
    if auto_activate:
        activated_at = _utcnow()
        await _publish_ptg2_source_pointers(
            source_key=source_key,
            snapshot_id=snapshot_id,
            previous_snapshot_id=previous_snapshot_id,
            import_month=import_month,
            updated_at=activated_at,
            snapshot_attributes=activated_snapshot_attributes(
                candidate_attributes,
                activated_at=activated_at,
                activation_mode="automatic",
            ),
        )
        activation_status = "activated"
    else:
        activation_status = "deferred"
    release_current_artifact_lease()
    post_publish_stage_timer.mark("logical_candidate_and_optional_pointer_cutover")
    if manifest_stage_table:
        await _drop_ptg2_snapshot_table_names(
            _ptg2_manifest_stage_table_names(manifest_stage_table)
        )
    post_publish_stage_timer.mark("scratch_cleanup")
    if auto_activate:
        await _cleanup_old_ptg2_source_tables(
            source_key,
            {snapshot_id},
            lock_pointer_state=True,
        )
    post_publish_stage_timer.mark("old_state_cleanup")
    address_refresh = (
        await _enqueue_ptg2_auto_address_refresh_after_import(
            source_key=source_key,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=test_mode,
        )
        if auto_activate
        else {"status": "skipped", "reason": "candidate-activation-deferred"}
    )
    post_publish_stage_timer.mark("address_refresh")
    publish_report_map["address_refresh"] = address_refresh
    publish_report_map["activation_status"] = activation_status
    await _persist_completed_ptg2_import_run(
        import_run_id=import_run_id,
        import_month=import_month,
        started_at=started_at,
        options=options,
        report_payload=publish_report_map,
        timing_payload=timings_by_phase,
        import_started_monotonic=import_started_monotonic,
        post_publish_started_monotonic=post_publish_started_monotonic,
        post_publish_stage_timer=post_publish_stage_timer,
    )
    write_live_progress(
        status="succeeded",
        phase="succeeded",
        unit="files",
        done=len(downloaded_jobs),
        total=len(downloaded_jobs),
        pct=100,
        eta_seconds=0,
        message="PTG import reused an identical PostgreSQL layout",
    )
    return {
        "status": "succeeded",
        "publish_status": "shared_layout_reused",
        "already_published": False,
        "shared_layout_reused": True,
        "activation_status": activation_status,
        "snapshot_status": (
            PTG2_STATUS_PUBLISHED if auto_activate else PTG2_STATUS_VALIDATED
        ),
        "shared_snapshot_key": int(shared_snapshot_key),
        "import_run_id": import_run_id,
        "snapshot_id": snapshot_id,
        "source_key": source_key,
        "import_month": import_month.isoformat(),
        "files_attempted": len(downloaded_jobs),
        "files_processed": 0,
        "files_reused": len(downloaded_jobs),
        "files_failed": 0,
        "serving_rates": rate_count,
        "rate_count": rate_count,
        "source_file_versions": source_file_versions,
        "address_refresh": address_refresh,
        "timings": timings_by_phase,
    }


async def _main_with_artifact_lease(
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
    import_started_monotonic = _ptg2_monotonic()
    import_month_value = normalize_import_month(import_month)
    source_key_val = _normalize_source_key(source_key or os.getenv("HLTHPRT_PTG2_SOURCE_KEY"))
    snapshot_arch_version = _ptg2_snapshot_arch_from_env()
    if provider_ref_url:
        raise ValueError(
            "provider_ref_url is not supported by strict V3; provider references "
            "must come from each in-network source"
        )
    if allowed_url:
        raise ValueError(
            "allowed_url is not supported by strict V3; only in-network rate files "
            "participate in a serving snapshot"
        )
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
            arch_variant=PTG2_V3_SHARED_GENERATION,
        )
    )
    import_run_id = f"ptg2:{import_id_val}"
    if source_key_val is None:
        if test_mode:
            source_key_val = _normalize_source_key(import_id_val)
        else:
            raise ValueError("PTG imports require --source-key or HLTHPRT_PTG2_SOURCE_KEY")
    assert source_key_val is not None
    source_network_name_values = sorted(
        _normalize_source_network_names(source_network_names),
        key=str.casefold,
    )
    auto_activate_candidates = _ptg2_auto_activate_candidates()
    options_payload = {
        "toc_urls": toc_urls or [],
        "toc_list": toc_list,
        "in_network_url": in_network_url,
        "allowed_url": allowed_url,
        "source_key": source_key_val,
        "plan_ids": plan_ids or [],
        "plan_name_contains": plan_name_contains or [],
        "plan_market_types": plan_market_types or [],
        "file_url_contains": file_url_contains or [],
        "source_network_names": source_network_name_values,
        "max_files": max_files,
        "reuse_raw_artifacts": reuse_raw_artifacts,
        "keep_partial_artifacts": _env_bool(PTG2_KEEP_PARTIAL_ENV, True)
        if keep_partial_artifacts is None else keep_partial_artifacts,
        "snapshot_arch": snapshot_arch_version,
        "storage_generation": PTG2_V3_SHARED_GENERATION,
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
        "auto_activate_candidates": auto_activate_candidates,
    }
    snapshot_id = _ptg2_deterministic_snapshot_id(
        import_month=import_month_value,
        import_id=import_id_val,
        option_by_name=options_payload,
    )
    live_run_id = str(control_run_id or "").strip()
    live_token = set_live_progress_context(
        run_id=live_run_id,
        source_key=source_key_val,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
    )
    setup_stage_timings: dict[str, float] = {}
    setup_stage_timer = _StageTimer(setup_stage_timings, import_started_monotonic)
    pending_strict_v3 = _PendingStrictV3State({}, {})

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
    setup_stage_timer.mark("ensure_database")
    await ensure_ptg2_tables()
    setup_stage_timer.mark("ensure_ptg2_tables")
    source_import_lock = _ptg2_source_import_lock(source_key_val)
    has_source_import_lock = False
    try:
        await source_import_lock.__aenter__()
        has_source_import_lock = True
        setup_stage_timer.mark("source_import_lock")
        observed_source_snapshot_id = await _current_source_snapshot_id(source_key_val)
        setup_stage_timer.mark("source_snapshot_lookup")
    except BaseException:
        if has_source_import_lock:
            await source_import_lock.__aexit__(None, None, None)
        reset_live_progress_context(live_token)
        raise
    now = _utcnow()
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
        )
    except BaseException:
        await source_import_lock.__aexit__(None, None, None)
        reset_live_progress_context(live_token)
        raise
    if snapshot_state and snapshot_state.get("status") == PTG2_STATUS_PUBLISHED:
        try:
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
            write_live_progress(
                status="succeeded",
                phase="succeeded",
                pct=100,
                eta_seconds=0,
                message="PTG snapshot already published; pointers reconciled",
            )
            return already_published_result
        finally:
            await source_import_lock.__aexit__(None, None, None)
            reset_live_progress_context(live_token)
    if snapshot_state and snapshot_state.get("status") == PTG2_STATUS_VALIDATED:
        try:
            candidate_result = await _resume_validated_candidate(
                snapshot_attributes=snapshot_state,
                snapshot_id=snapshot_id,
                source_key=source_key_val,
                import_month=import_month_value,
                auto_activate=auto_activate_candidates,
            )
            if auto_activate_candidates:
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
                    await _enqueue_ptg2_auto_address_refresh_after_import(
                        source_key=source_key_val,
                        snapshot_id=snapshot_id,
                        import_run_id=import_run_id,
                        has_serving_files=True,
                        source_scoped_compact=True,
                        test_mode=test_mode,
                    )
                )
            write_live_progress(
                status="succeeded",
                phase="succeeded",
                pct=100,
                eta_seconds=0,
                message=(
                    "PTG candidate activated"
                    if auto_activate_candidates
                    else "PTG candidate already validated; live pointers unchanged"
                ),
            )
            return candidate_result
        finally:
            await source_import_lock.__aexit__(None, None, None)
            reset_live_progress_context(live_token)
    if snapshot_state and snapshot_state.get("snapshot_claim_status") == "existing":
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
    failure_report: dict[str, Any] = {"snapshot_id": snapshot_id, "legacy_table_suffix": import_id_val}
    ptg2_manifest_stage_table: str | None = None
    ptg2_import_heartbeat_task: asyncio.Task[Any] | None = None
    shared_layout_reservation = None
    shared_input_identity = None
    shared_layout_build_token = uuid.uuid4().hex
    previous_snapshot_id = (
        str(observed_source_snapshot_id) if observed_source_snapshot_id else None
    )
    current_pointer_published = False
    candidate_stage_state = {"staged": False}

    async def mark_import_failed(error: BaseException | str, *, progress_message: str | None = None) -> None:
        """Persist import failure state and drop unpublished source-scoped staging tables."""
        failure_handling_started_monotonic = _ptg2_monotonic()
        error_text = str(error) or "worker task was cancelled"
        write_live_progress(
            phase="failing",
            pct=99,
            message="persisting PTG import failure state",
        )
        serving_index = failure_report.get("serving_index")
        is_snapshot_known_published = current_pointer_published
        should_preserve_candidate_tables = (
            current_pointer_published or candidate_stage_state["staged"]
        )
        if not should_preserve_candidate_tables and isinstance(serving_index, dict):
            try:
                is_snapshot_known_published = (
                    await _current_source_snapshot_id(source_key_val) == snapshot_id
                )
                should_preserve_candidate_tables = is_snapshot_known_published
            except Exception:
                should_preserve_candidate_tables = True
                logger.warning(
                    "Could not recheck the PTG source pointer during failure handling; "
                    "preserving candidate tables to avoid deleting live data",
                    exc_info=True,
                )
        if not should_preserve_candidate_tables:
            await _cleanup_failed_ptg2_source_state(
                serving_index=(serving_index if isinstance(serving_index, dict) else None),
                manifest_stage_table=ptg2_manifest_stage_table,
                snapshot_id=snapshot_id,
            )
            abandoned_layout = await _is_failed_shared_layout_abandoned(
                shared_layout_reservation,
                build_token=shared_layout_build_token,
            )
            if abandoned_layout is not None:
                failure_report["shared_layout_abandoned"] = abandoned_layout
            elif shared_layout_reservation is not None and not shared_layout_reservation.reused:
                failure_report["shared_layout_abandoned"] = False
                failure_report["shared_layout_abandonment_deferred"] = True
        _cleanup_manifest_copy_entries(pending_strict_v3.copy_entries_by_kind)
        _cleanup_strict_v3_graph_artifacts(pending_strict_v3.graph_artifacts_map)
        pending_strict_v3.copy_entries_by_kind = {}
        pending_strict_v3.graph_artifacts_map = {}
        await _mark_ptg2_import_failed(
            import_run_id,
            snapshot_id,
            import_month_value,
            now,
            error_text,
            report=failure_report,
            options=options_payload,
            should_preserve_published_snapshot=(
                is_snapshot_known_published or candidate_stage_state["staged"]
            ),
            import_started_monotonic=import_started_monotonic,
            failure_handling_started_monotonic=failure_handling_started_monotonic,
        )
        write_live_progress(
            status="failed",
            phase="failed",
            pct=100,
            eta_seconds=0,
            message=progress_message or "PTG import failed; inspect worker logs",
        )

    try:
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
        ptg2_import_heartbeat_task = asyncio.create_task(
            _heartbeat_ptg2_import_run(import_run_id),
            name=f"ptg2-import-heartbeat:{import_run_id}",
        )
        setup_stage_timer.mark("initial_status_rows")
        assert source_key_val is not None
        write_live_progress(phase="planning", pct=3, message="planning PTG files")
        stage_token = _ptg2_snapshot_table_token(source_key_val, snapshot_id)
        ptg2_manifest_stage_table = _ptg2_manifest_stage_table_name(stage_token)
        ptg2_manifest_stage_table = await _create_ptg2_manifest_serving_stage_table(stage_token)
        setup_stage_timer.mark("manifest_stage_table")
        classes = await _prepare_ptg_tables(
            import_id_val,
            test_mode,
            initial_table_class_names=set(PTG_CONTROL_TABLE_CLASS_NAMES),
        )
        setup_stage_timer.mark("control_tables")

        jobs: list[dict[str, Any]] = []
        data_started_monotonic = _ptg2_monotonic()

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

        if in_network_url:
            job: dict[str, Any] = {"type": "in_network", "url": in_network_url}
            if source_network_name_values:
                job["source_network_names"] = source_network_name_values
            jobs.append(job)
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
        if toc_failures:
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
                "strict V3 never publishes partial source coverage"
            )
        if toc_candidates and not jobs and not in_network_url:
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
            if job.get("type") == "in_network":
                selected_jobs.append(job)
        if not selected_jobs:
            raise RuntimeError(
                "strict V3 import discovered no supported in-network rate files"
            )
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
        processing_tasks: set[asyncio.Task[PTG2FileProcessResult | None]] = set()

        async def record_file_result(result: PTG2FileProcessResult | None) -> None:
            """Classify a file result and update completion progress."""
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
            """Drain queued processing tasks as capacity requires and record results."""
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
            """Map one job's index onto its allocated overall progress interval."""
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
            """Process a downloaded in-network artifact under its progress context."""
            job = downloaded.job
            token = set_live_progress_context(**file_progress_context(job, start_pct=20.0, end_pct=90.0))
            try:
                if job.get("type") == "in_network":
                    if shared_input_identity is None:
                        raise RuntimeError("strict V3 physical input identity was not established")
                    file_result = await _process_in_network_file(
                        job,
                        classes,
                        test_mode,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        max_items=max_items,
                        import_run_id=import_run_id,
                        keep_partial_artifacts=keep_partial_artifacts,
                        snapshot_id=snapshot_id,
                        coverage_scope_id=shared_input_identity.coverage_scope_hex,
                        import_month=import_month_value,
                        ptg2_manifest_stage_table=ptg2_manifest_stage_table,
                        source_network_names=job.get("source_network_names"),
                        raw_artifact=downloaded.raw_artifact,
                        logical_artifact=downloaded.logical_artifact,
                    )
                    return _annotate_v3_file_result_source_identity(
                        file_result,
                        shared_physical_artifact_identity(downloaded),
                        shared_logical_artifact_metadata(downloaded),
                    )
                return None
            finally:
                reset_live_progress_context(token)

        try:
            buffered_downloads: list[PTG2DownloadedJob] = []
            async for downloaded in _iter_downloaded_ptg_jobs(
                selected_jobs,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            ):
                buffered_downloads.append(downloaded)
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
                failed_files.extend(asdict(result) for result in download_failures)
                failure_report.update(
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
                    "strict V3 requires successful in-network downloads with one logical plan scope"
                )
            write_live_progress(
                phase="planning",
                unit="files",
                done=len(buffered_downloads),
                total=len(buffered_downloads),
                pct=20,
                message="checking for an identical shared PostgreSQL layout",
            )
            shared_input_identity = shared_physical_input_identity(
                buffered_downloads,
                options=options_payload,
                scanner_canon_version=_shared_v3_scanner_identity(),
            )
            canonical_plan_values_by_field = {
                key: value
                for key, value in shared_input_identity.logical_plan_fields.items()
                if value is not None and str(value).strip()
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
                shared_layout_reservation = await reserve_shared_layout(
                    session,
                    schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
                    semantic_fingerprint=shared_input_identity.semantic_fingerprint,
                    build_token=shared_layout_build_token,
                )
            failure_report.update(
                {
                    "shared_snapshot_key": shared_layout_reservation.snapshot_key,
                    "shared_semantic_fingerprint": (
                        shared_input_identity.semantic_fingerprint.hex()
                    ),
                    "coverage_scope_id": shared_input_identity.coverage_scope_hex,
                    "shared_layout_reused": shared_layout_reservation.reused,
                }
            )
            if shared_layout_reservation.reused:
                return await _publish_reused_shared_v3_snapshot(
                    downloaded_jobs=buffered_downloads,
                    shared_input_identity=shared_input_identity,
                    classes=classes,
                    layout_manifest=shared_layout_reservation.layout_manifest,
                    shared_snapshot_key=shared_layout_reservation.snapshot_key,
                    semantic_fingerprint=shared_input_identity.semantic_fingerprint,
                    coverage_scope_id=shared_input_identity.coverage_scope_id,
                    coverage_plan_id=shared_input_identity.logical_plan.plan_id,
                    coverage_plan_market_type=(
                        shared_input_identity.logical_plan.plan_market_type
                    ),
                    snapshot_id=snapshot_id,
                    import_run_id=import_run_id,
                    source_key=source_key_val,
                    import_month=import_month_value,
                    previous_snapshot_id=previous_snapshot_id,
                    started_at=now,
                    options=options_payload,
                    manifest_stage_table=ptg2_manifest_stage_table,
                    test_mode=test_mode,
                    import_started_monotonic=import_started_monotonic,
                    candidate_stage_state=candidate_stage_state,
                )

            async def iter_downloaded_jobs():
                """Yield the fully validated download batch in discovery order."""

                for buffered_download in buffered_downloads:
                    yield buffered_download

            downloaded_jobs = iter_downloaded_jobs()

            async for downloaded in downloaded_jobs:
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
                elif any(
                    same_downloaded_physical_input(previous, downloaded)
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
                    _emit_screen_line(
                        "PTG2_RAW_JOB_DEDUPE"
                        f"\ttype={job.get('type')}"
                        f"\turl={job.get('url')}"
                        f"\traw_sha256={downloaded.raw_artifact.raw_sha256}"
                        f"\tlogical_sha256={downloaded.logical_artifact.logical_sha256}"
                        "\treason=duplicate_logical_artifact"
                    )
                    result = PTG2FileProcessResult(
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
                    result = _annotate_v3_file_result_source_identity(
                        result,
                        shared_physical_artifact_identity(downloaded),
                        shared_logical_artifact_metadata(downloaded),
                    )
                elif downloaded.logical_artifact.logical_sha256:
                    downloads_by_logical_hash.setdefault(
                        downloaded.logical_artifact.logical_sha256,
                        [],
                    ).append(downloaded)
                if result is not None:
                    await record_file_result(result)
                    continue

                processing_tasks.add(asyncio.create_task(process_downloaded_job(downloaded)))
                await drain_processing_tasks()
            await drain_processing_tasks(force=True)
        finally:
            await _cancel_and_wait_tasks(processing_tasks)

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
        if shared_layout_reservation is not None:
            failure_report.update(
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
        pending_strict_v3.copy_entries_by_kind = _collect_manifest_copy_entries(
            successful_files,
            (
                "serving_run",
                "serving_code_dictionary",
                "provider_set_metadata",
            ),
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

        if shared_input_identity is None:
            raise RuntimeError("strict V3 source publication is missing physical input identity")
        source_identity_traces = _shared_v3_identity_trace_pairs_from_results(
            successful_files + skipped_files
        )
        source_set = _shared_v3_source_set_metadata(
            source_identity_traces,
            expected_source_count=shared_input_identity.source_count,
        )
        provider_identifier_quarantine = (
            _shared_v3_provider_identifier_quarantine(successful_files)
        )
        await _publish_shared_v3_source_dictionary(
            shared_input_identity=shared_input_identity,
            identity_trace_pairs=source_identity_traces,
            snapshot_id=snapshot_id,
        )

        await flush_error_log(classes["ImportLog"])
        data_seconds = _ptg2_monotonic() - data_started_monotonic
        publish_started_monotonic = _ptg2_monotonic()
        write_live_progress(phase="publishing", pct=92, message="publishing PTG snapshot")
        publish_progress_total = 8
        _emit_ptg2_publish_progress(
            "starting",
            completed_steps=0,
            total_steps=publish_progress_total,
            message_text="starting PTG snapshot publish",
        )
        manifest_merge_metrics: dict[str, Any] = {"enabled": False}
        manifest_precopy_merge_seconds = 0.0
        has_serving_files = any(
            file_summary.get("source_type") == "in_network" and not file_summary.get("skipped")
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
            )
            manifest_precopy_merge_started_monotonic = _ptg2_monotonic()
            manifest_merge_metrics = await _merge_and_copy_ptg2_manifest_files(
                successful_files=successful_files,
                manifest_stage_table=ptg2_manifest_stage_table,
            )
            manifest_precopy_merge_seconds = (
                _ptg2_monotonic() - manifest_precopy_merge_started_monotonic
            )
            manifest_merge_metrics["elapsed_seconds"] = manifest_precopy_merge_seconds
            _emit_ptg2_publish_progress(
                "pre-copy merge complete",
                completed_steps=4,
                total_steps=publish_progress_total,
                message_text="manifest copy files loaded into staging tables",
                serving_rows=manifest_merge_metrics.get("serving_rows"),
                streamed_to_copy=manifest_merge_metrics.get("streamed_to_copy"),
            )
            for file_summary in successful_files:
                summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
                manifest_payload = summary_payload.get("manifest") if isinstance(summary_payload, dict) else None
                if isinstance(manifest_payload, dict):
                    manifest_payload.pop("copy_files", None)
        manifest_artifacts = _collect_manifest_artifacts(
            successful_files + skipped_files
        )
        pending_strict_v3.graph_artifacts_map = manifest_artifacts
        assert source_key_val is not None
        if has_serving_files:
            if not ptg2_manifest_stage_table:
                raise RuntimeError("PTG import did not create a manifest-backed serving stage table")
            _emit_ptg2_publish_progress(
                "publishing snapshot tables",
                completed_steps=5,
                total_steps=publish_progress_total,
                message_text="publishing PTG manifest snapshot tables",
            )
            if shared_layout_reservation is None or shared_input_identity is None:
                raise RuntimeError("strict V3 publish is missing its physical input reservation")
            run_entries = strict_v3_copy_entries.get("serving_run") or []
            code_dictionary_entries = strict_v3_copy_entries.get(
                "serving_code_dictionary"
            ) or []
            provider_set_metadata_entries = strict_v3_copy_entries.get(
                "provider_set_metadata"
            ) or []
            try:
                shared_publication = await publish_strict_shared_v3_layout(
                    schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
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
                    graph_artifact_entries=list(manifest_artifacts.get("sidecars") or []),
                    provider_identifier_quarantine=provider_identifier_quarantine,
                    scratch_parent=ptg2_temp_parent(),
                )
            finally:
                _cleanup_manifest_copy_entries(strict_v3_copy_entries)
                _cleanup_strict_v3_graph_artifacts(manifest_artifacts)
                pending_strict_v3.copy_entries_by_kind = {}
                pending_strict_v3.graph_artifacts_map = {}
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
            manifest_merge_metrics["serving_rows"] = serving_index.get(
                "serving_rates"
            )
            failure_report.update(
                {
                    "shared_snapshot_key": shared_publication.snapshot_key,
                    "shared_layout_reused_at_seal": (
                        shared_publication.layout_reused_at_seal
                    ),
                    "shared_stored_byte_count": shared_publication.stored_byte_count,
                }
            )
            await _drop_ptg2_snapshot_table_names(
                _ptg2_manifest_stage_table_names(ptg2_manifest_stage_table)
            )
            ptg2_manifest_stage_table = None
            failure_report["serving_index"] = serving_index
            _emit_ptg2_publish_progress(
                "snapshot tables published",
                completed_steps=6,
                total_steps=publish_progress_total,
                message_text="PTG manifest snapshot tables published",
                serving_rates=serving_index.get("serving_rates") if isinstance(serving_index, dict) else None,
                rate_count=serving_index.get("rate_count") if isinstance(serving_index, dict) else None,
            )
        publish_seconds = _ptg2_monotonic() - publish_started_monotonic
        post_publish_started_monotonic = _ptg2_monotonic()
        post_publish_stage_timings: dict[str, float] = {}
        post_publish_stage_timer = _StageTimer(
            post_publish_stage_timings,
            post_publish_started_monotonic,
        )

        validated_at = _utcnow()
        serving_timings = serving_index.get("timings", {}) if isinstance(serving_index, dict) else {}
        setup_seconds = data_started_monotonic - import_started_monotonic
        timing_payload = {
            "setup_seconds": setup_seconds,
            "data_seconds": data_seconds,
            "publish_seconds": publish_seconds,
            "manifest_precopy_merge_seconds": manifest_precopy_merge_seconds,
        }
        for key, value in setup_stage_timings.items():
            timing_payload[f"setup_{key}_seconds"] = value
        if isinstance(serving_timings, dict):
            for key, value in serving_timings.items():
                try:
                    timing_key = f"serving_{key}" if key in timing_payload else key
                    timing_payload[timing_key] = float(value)
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
                **report_payload,
                "timings": dict(timing_payload),
            },
        }
        if not isinstance(serving_index, dict) or serving_index.get("shared_snapshot_key") is None:
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
            coverage_plan_id=shared_input_identity.logical_plan.plan_id,
            coverage_plan_market_type=(
                shared_input_identity.logical_plan.plan_market_type
            ),
        )
        candidate_stage_state["staged"] = True
        candidate_attributes = dict(candidate_result["candidate_attributes"])
        if auto_activate_candidates:
            activated_at = _utcnow()
            await _publish_ptg2_source_pointers(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                import_month=import_month_value,
                updated_at=activated_at,
                snapshot_attributes=activated_snapshot_attributes(
                    candidate_attributes,
                    activated_at=activated_at,
                    activation_mode="automatic",
                ),
            )
            current_pointer_published = True
            activation_status = "activated"
            snapshot_status = PTG2_STATUS_PUBLISHED
        else:
            activation_status = "deferred"
            snapshot_status = PTG2_STATUS_VALIDATED
        release_current_artifact_lease()
        post_publish_stage_timer.mark(
            "logical_candidate_and_optional_pointer_cutover"
        )
        _emit_ptg2_publish_progress(
            "cleaning old source tables",
            completed_steps=7,
            total_steps=publish_progress_total,
            message_text="cleaning old PTG source tables",
        )
        if auto_activate_candidates:
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
            await _enqueue_ptg2_auto_address_refresh_after_import(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
                has_serving_files=True,
                source_scoped_compact=True,
                test_mode=test_mode,
            )
            if auto_activate_candidates
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
            address_refresh_status=address_refresh_result.get("status") if isinstance(address_refresh_result, dict) else None,
        )
        report_payload["address_refresh"] = address_refresh_result
        report_payload["activation_status"] = activation_status
        await _persist_completed_ptg2_import_run(
            import_run_id=import_run_id,
            import_month=import_month_value,
            started_at=now,
            options=options_payload,
            report_payload=report_payload,
            timing_payload=timing_payload,
            import_started_monotonic=import_started_monotonic,
            post_publish_started_monotonic=post_publish_started_monotonic,
            post_publish_stage_timer=post_publish_stage_timer,
        )
        _emit_ptg2_publish_progress(
            "validated",
            completed_steps=8,
            total_steps=publish_progress_total,
            message_text="PTG publish validation complete",
            address_refresh_status=address_refresh_result.get("status") if isinstance(address_refresh_result, dict) else None,
        )
        done_line = (
            "PTG2_IMPORT_DONE"
            f"\timport_run_id={import_run_id}"
            f"\tsnapshot_id={snapshot_id}"
            f"\tstatus={snapshot_status}"
            f"\tactivation_status={activation_status}"
            f"\tfiles_processed={processed_file_count_map['done']}"
            f"\tfiles_failed={len(failed_files)}"
            f"\tserving_rates={report_payload.get('serving_rates', 'unknown')}"
            f"\ttotal_seconds={timing_payload['total_seconds']:.2f}"
            f"\tsetup_seconds={timing_payload['setup_seconds']:.2f}"
            f"\tdata_seconds={timing_payload['data_seconds']:.2f}"
            f"\tpublish_seconds={timing_payload['publish_seconds']:.2f}"
            f"\tpost_publish_seconds={timing_payload['post_publish_seconds']:.2f}"
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
            message=(
                "PTG import succeeded"
                if auto_activate_candidates
                else "PTG candidate validated; live pointers unchanged"
            ),
        )
        return {
            "status": "succeeded",
            "arch_version": "postgres_binary_v3",
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
            "serving_rates": report_payload.get("serving_rates"),
            "rate_count": report_payload.get("rate_count"),
            "source_file_versions": _ptg2_source_file_versions_from_results(successful_files + skipped_files),
            "address_refresh": address_refresh_result,
            "timings": timing_payload,
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
        await _stop_ptg2_import_heartbeat(ptg2_import_heartbeat_task)
        _cleanup_manifest_copy_entries(pending_strict_v3.copy_entries_by_kind)
        _cleanup_strict_v3_graph_artifacts(pending_strict_v3.graph_artifacts_map)
        try:
            await source_import_lock.__aexit__(None, None, None)
        except Exception:
            logger.warning("Failed to release PTG2 source import lock", exc_info=True)
        reset_live_progress_context(live_token)


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
    """Run one PTG import while retaining shared inputs through a live lease."""

    lease_owner = str(
        control_run_id
        or import_id
        or source_key
        or f"standalone-{uuid.uuid4().hex}"
    )
    with artifact_lease_context(owner=f"ptg:{lease_owner}") as lease:
        return await guard_artifact_lease(
            lease,
            _main_with_artifact_lease(
                test_mode=test_mode,
                toc_urls=toc_urls,
                toc_list=toc_list,
                in_network_url=in_network_url,
                allowed_url=allowed_url,
                provider_ref_url=provider_ref_url,
                import_id=import_id,
                source_key=source_key,
                import_month=import_month,
                max_files=max_files,
                max_items=max_items,
                plan_ids=plan_ids,
                plan_name_contains=plan_name_contains,
                plan_market_types=plan_market_types,
                file_url_contains=file_url_contains,
                source_network_names=source_network_names,
                reuse_raw_artifacts=reuse_raw_artifacts,
                keep_partial_artifacts=keep_partial_artifacts,
                control_run_id=control_run_id,
            ),
        )


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
    source_inputs = {
        "source_key": source_key_val,
        "toc_urls": toc_urls or [],
        "toc_list": toc_list or "",
        "in_network_url": in_network_url or "",
        "allowed_url": allowed_url or "",
        "provider_ref_url": provider_ref_url or "",
        "arch_variant": arch_variant or "",
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
        return [dict(sidecar) for sidecar in raw_sidecars.values() if isinstance(sidecar, dict)]
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


def _collect_manifest_artifacts(
    successful_files: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate manifest sidecars, trace identity, and network names by source shard."""

    sidecar_entries: list[dict[str, Any]] = []
    source_trace_hashes: set[str] = set()
    fallback_source_trace_set_hashes: set[str] = set()
    network_names: set[str] = set()
    for file_index, file_summary in enumerate(successful_files):
        summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
        if not isinstance(summary_payload, dict):
            continue
        manifest_payload = summary_payload.get("manifest")
        if not isinstance(manifest_payload, dict):
            continue
        source_trace_hash = str(manifest_payload.get("source_trace_hash") or "").strip()
        if source_trace_hash:
            source_trace_hashes.add(source_trace_hash)
        else:
            source_trace_set_hash = str(
                manifest_payload.get("source_trace_set_hash") or ""
            ).strip()
            if source_trace_set_hash:
                fallback_source_trace_set_hashes.add(source_trace_set_hash)
        network_names.update(
            _normalize_source_network_names(manifest_payload.get("network_names") or [])
        )
        source_shard_id = _manifest_source_shard_id(file_summary, summary_payload, file_index)
        existing_sidecars = _manifest_sidecars_list(manifest_payload)
        if existing_sidecars:
            for sidecar in existing_sidecars:
                sidecar["source_shard_id"] = source_shard_id
                sidecar_entries.append(sidecar)
            continue
        raw_sidecar_path_map = manifest_payload.get("sidecar_paths")
        if not isinstance(raw_sidecar_path_map, dict):
            continue
        sidecar_path_map: dict[str, Path | None] = {}
        for name, raw_path in raw_sidecar_path_map.items():
            path = Path(str(raw_path)) if raw_path else None
            sidecar_path_map[str(name)] = path
        fallback_sidecar_map = _collect_ptg2_manifest_sidecar_artifacts(sidecar_path_map)
        for sidecar in fallback_sidecar_map.values():
            sidecar_map = dict(sidecar)
            sidecar_map["source_shard_id"] = source_shard_id
            sidecar_entries.append(sidecar_map)
    artifacts: dict[str, Any] = {"sidecars": sidecar_entries} if sidecar_entries else {}
    if source_trace_hashes:
        artifacts["source_trace_set_hash"] = build_source_trace_set(
            sorted(source_trace_hashes)
        )["source_trace_set_hash"]
    elif len(fallback_source_trace_set_hashes) == 1:
        artifacts["source_trace_set_hash"] = next(
            iter(fallback_source_trace_set_hashes)
        )
    if network_names:
        artifacts["network_names"] = sorted(network_names, key=str.casefold)
    return artifacts
