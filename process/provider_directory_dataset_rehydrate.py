# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Restore typed rows from one exact current Provider Directory dataset."""

from __future__ import annotations

import dataclasses
from typing import Any

from sqlalchemy import types as sa_types

from process.provider_directory_dataset_rehydrate_report import (
    _execution_summary,
    _resource_summary,
)
from process.provider_directory_dataset_rehydrate_scope import (
    _dataset_guard,
    _load_dataset_scope,
    _selected_resource_types,
    _verify_dataset_digest,
)
from process.provider_directory_dataset_rehydrate_store import (
    _is_proof_complete,
    _load_checkpoint,
    _load_resource_proof,
    _save_checkpoint,
)
from process.provider_directory_dataset_rehydrate_types import (
    DATASET_RESOURCE_TABLE,
    DEFAULT_BATCH_SIZE,
    MAX_BATCH_SIZE,
    DatasetRehydrationError,
    DatasetScope,
    RehydrationCheckpoint,
    RehydrationRequest,
    RehydrationRuntime,
    ResourceContext,
    RetainedBatch,
    _clean_text,
    _payload_hash,
    _record_fields,
    _table_ref,
)


async def rehydrate_current_dataset(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
) -> dict[str, Any]:
    """Restore an exact current dataset without network access."""
    _validate_request(request)
    async with _dataset_guard(runtime, request):
        dataset_scope = await _load_dataset_scope(runtime, request)
        selected_resource_types = _selected_resource_types(
            runtime, request, dataset_scope
        )
        digest_before = await _verify_dataset_digest(runtime, dataset_scope)
        summary_by_resource = await _rehydrate_selected_resources(
            runtime, request, dataset_scope, selected_resource_types
        )
        digest_after = await _verify_dataset_digest(runtime, dataset_scope)
        await _assert_scope_unchanged(runtime, request, dataset_scope)
    return _execution_summary(
        request, dataset_scope, summary_by_resource, digest_before, digest_after
    )


def _validate_request(request: RehydrationRequest) -> None:
    """Reject incomplete identity and unbounded batch options."""
    identity_parts = (
        request.source_id,
        request.dataset_id,
        request.acquisition_root_run_id,
        request.owner_run_id,
    )
    if any(not _clean_text(identity_part) for identity_part in identity_parts):
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_identity_required"
        )
    if not 1 <= request.batch_size <= MAX_BATCH_SIZE:
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_batch_size_invalid"
        )


async def _rehydrate_selected_resources(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    scope: DatasetScope,
    selected_resource_types: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    """Process selected resource types serially under the dataset guard."""
    summary_by_resource: dict[str, dict[str, Any]] = {}
    for resource_type in selected_resource_types:
        resource_context = await _resource_context(
            runtime, request, scope, resource_type
        )
        summary_by_resource[resource_type] = await _rehydrate_resource(
            resource_context
        )
    return summary_by_resource


async def _resource_context(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    scope: DatasetScope,
    resource_type: str,
) -> ResourceContext:
    """Build the immutable context for one resource type."""
    expected_count = int(
        await runtime.database.scalar(
            f"SELECT count(*) FROM "
            f"{_table_ref(runtime.schema, DATASET_RESOURCE_TABLE)} "
            "WHERE dataset_id=:dataset_id AND resource_type=:resource_type;",
            dataset_id=scope.dataset_id,
            resource_type=resource_type,
        )
        or 0
    )
    return ResourceContext(
        runtime=runtime,
        request=request,
        scope=scope,
        resource_type=resource_type,
        model=runtime.models_by_type[resource_type],
        expected_count=expected_count,
    )


async def _rehydrate_resource(context: ResourceContext) -> dict[str, Any]:
    """Resume, process, and prove one retained resource type."""
    checkpoint, reused_summary = await _prepare_checkpoint(context)
    if reused_summary is not None:
        return reused_summary
    try:
        checkpoint = await _process_resource_batches(context, checkpoint)
        return await _finalize_resource(context, checkpoint)
    except BaseException as processing_error:
        await _mark_interrupted(context, processing_error)
        raise


async def _prepare_checkpoint(
    context: ResourceContext,
) -> tuple[RehydrationCheckpoint, dict[str, Any] | None]:
    """Reuse complete proof, resume a valid prefix, or initialize progress."""
    stored_checkpoint = await _load_checkpoint(context)
    resource_proof = await _load_resource_proof(context)
    if (
        stored_checkpoint is not None
        and stored_checkpoint.state == "complete"
        and _is_proof_complete(stored_checkpoint, resource_proof)
    ):
        return stored_checkpoint, _resource_summary(
            stored_checkpoint, resource_proof, reused=True
        )
    resumable_checkpoint = await _resumable_checkpoint(context, stored_checkpoint)
    await _save_checkpoint(context, resumable_checkpoint)
    return resumable_checkpoint, None


async def _resumable_checkpoint(
    context: ResourceContext,
    stored_checkpoint: RehydrationCheckpoint | None,
) -> RehydrationCheckpoint:
    """Adopt only a checkpoint whose cursor still matches its input prefix."""
    if (
        stored_checkpoint is None
        or stored_checkpoint.state not in {"running", "interrupted"}
        or stored_checkpoint.expected_count != context.expected_count
        or stored_checkpoint.last_resource_id is None
    ):
        return _empty_checkpoint(context)
    prefix_count = int(
        await context.runtime.database.scalar(
            f"SELECT count(*) FROM "
            f"{_table_ref(context.runtime.schema, DATASET_RESOURCE_TABLE)} "
            "WHERE dataset_id=:dataset_id AND resource_type=:resource_type "
            "AND resource_id<=:cursor;",
            dataset_id=context.scope.dataset_id,
            resource_type=context.resource_type,
            cursor=stored_checkpoint.last_resource_id,
        )
        or 0
    )
    if prefix_count != stored_checkpoint.input_count:
        return _empty_checkpoint(context)
    return dataclasses.replace(stored_checkpoint, state="running", error=None)


def _empty_checkpoint(context: ResourceContext) -> RehydrationCheckpoint:
    """Return initial progress for a fresh resource scan."""
    return RehydrationCheckpoint(
        state="running",
        last_resource_id=None,
        expected_count=context.expected_count,
        input_count=0,
        mapped_count=0,
        rejected_count=0,
    )


async def _process_resource_batches(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> RehydrationCheckpoint:
    """Process committed batches until the retained input is exhausted."""
    current_checkpoint = checkpoint
    while current_checkpoint.input_count < context.expected_count:
        await _check_cancel(context.runtime)
        current_checkpoint = await _process_one_batch(context, current_checkpoint)
        await _report_progress(context, current_checkpoint)
        if current_checkpoint.state == "rejected":
            raise DatasetRehydrationError(
                "provider_directory_dataset_rehydrate_payload_rejected"
            )
    return current_checkpoint


async def _process_one_batch(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> RehydrationCheckpoint:
    """Write rows and checkpoint progress in one database transaction."""
    async with context.runtime.database.transaction():
        await _assert_scope_unchanged(
            context.runtime, context.request, context.scope, lock=True
        )
        retained_records = await _read_retained_batch(context, checkpoint)
        retained_batch = _map_retained_batch(
            context.model, retained_records, context.scope
        )
        next_checkpoint = _advance_checkpoint(
            checkpoint, retained_batch, mapped_count=0
        )
        if retained_batch.rejection_reasons:
            await _save_checkpoint(context, next_checkpoint)
            return next_checkpoint
        mapped_count = await context.runtime.upsert_batch(
            context.model, retained_batch.typed_rows, context.scope
        )
        if mapped_count != len(retained_batch.typed_rows):
            raise DatasetRehydrationError(
                "provider_directory_dataset_rehydrate_upsert_count_mismatch"
            )
        next_checkpoint = _advance_checkpoint(
            checkpoint, retained_batch, mapped_count=mapped_count
        )
        await _save_checkpoint(context, next_checkpoint)
    return next_checkpoint


async def _read_retained_batch(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> list[Any]:
    """Read one ordered keyset page of retained mapped payloads."""
    retained_records = await context.runtime.database.all(
        f"""SELECT resource_id, payload_hash, payload_json
              FROM {_table_ref(context.runtime.schema, DATASET_RESOURCE_TABLE)}
             WHERE dataset_id=:dataset_id AND resource_type=:resource_type
               AND (:cursor IS NULL OR resource_id > :cursor)
             ORDER BY resource_id LIMIT :batch_size;""",
        dataset_id=context.scope.dataset_id,
        resource_type=context.resource_type,
        cursor=checkpoint.last_resource_id,
        batch_size=context.request.batch_size,
    )
    if not retained_records:
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_input_ended_early"
        )
    return retained_records


def _map_retained_batch(
    model: type,
    retained_records: list[Any],
    scope: DatasetScope,
) -> RetainedBatch:
    """Validate retained hashes and restore immutable acquisition metadata."""
    typed_rows: list[dict[str, Any]] = []
    rejection_reasons: list[str] = []
    for retained_record in retained_records:
        retained_fields = _record_fields(retained_record)
        resource_id = _clean_text(retained_fields.get("resource_id"))
        mapped_payload = retained_fields.get("payload_json")
        rejection_reason = _validate_payload(
            model,
            resource_id,
            _clean_text(retained_fields.get("payload_hash")),
            mapped_payload,
        )
        if rejection_reason:
            rejection_reasons.append(rejection_reason)
            continue
        typed_rows.append(
            {
                **mapped_payload,
                "source_id": scope.source_id,
                "last_seen_run_id": scope.acquisition_root_run_id,
                "observed_at": scope.published_at,
                "updated_at": scope.published_at,
            }
        )
    last_resource_id = _clean_text(
        _record_fields(retained_records[-1]).get("resource_id")
    )
    return RetainedBatch(
        typed_rows=typed_rows,
        rejection_reasons=tuple(rejection_reasons),
        input_count=len(retained_records),
        last_resource_id=last_resource_id,
    )


def _advance_checkpoint(
    checkpoint: RehydrationCheckpoint,
    retained_batch: RetainedBatch,
    *,
    mapped_count: int,
) -> RehydrationCheckpoint:
    """Return progress advanced through one retained batch."""
    rejection_count = len(retained_batch.rejection_reasons)
    evidence_by_name = (
        {
            "rejection_reasons": sorted(set(retained_batch.rejection_reasons)),
            "rejected_in_batch": rejection_count,
        }
        if rejection_count
        else {}
    )
    return RehydrationCheckpoint(
        state="rejected" if rejection_count else "running",
        last_resource_id=retained_batch.last_resource_id,
        expected_count=checkpoint.expected_count,
        input_count=checkpoint.input_count + retained_batch.input_count,
        mapped_count=checkpoint.mapped_count + mapped_count,
        rejected_count=checkpoint.rejected_count + rejection_count,
        evidence_by_name=evidence_by_name,
    )


async def _finalize_resource(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> dict[str, Any]:
    """Persist complete state only after independent exact-membership proof."""
    async with context.runtime.database.transaction():
        await _assert_scope_unchanged(
            context.runtime, context.request, context.scope, lock=True
        )
        resource_proof = await _load_resource_proof(context)
        final_state = (
            "complete"
            if _is_proof_complete(checkpoint, resource_proof)
            else "proof_failed"
        )
        final_checkpoint = dataclasses.replace(
            checkpoint,
            state=final_state,
            evidence_by_name={"proof": resource_proof.as_dict()},
        )
        await _save_checkpoint(context, final_checkpoint)
    if final_state != "complete":
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_proof_failed"
        )
    return _resource_summary(final_checkpoint, resource_proof, reused=False)


async def _mark_interrupted(
    context: ResourceContext,
    processing_error: BaseException,
) -> None:
    """Record interruption without advancing rolled-back batch progress."""
    try:
        stored_checkpoint = await _load_checkpoint(context)
        if stored_checkpoint is None or stored_checkpoint.state != "running":
            return
        interrupted_checkpoint = dataclasses.replace(
            stored_checkpoint,
            state="interrupted",
            error=f"{type(processing_error).__name__}: {str(processing_error)[:240]}",
        )
        await _save_checkpoint(context, interrupted_checkpoint)
    except Exception:
        return


async def _assert_scope_unchanged(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    expected_scope: DatasetScope,
    *,
    lock: bool = False,
) -> None:
    """Reject changes to any immutable dataset fence value."""
    current_scope = await _load_dataset_scope(runtime, request, lock=lock)
    if current_scope.fence_identity != expected_scope.fence_identity:
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_scope_changed"
        )


async def _check_cancel(runtime: RehydrationRuntime) -> None:
    """Invoke the optional control-plane cancellation callback."""
    if runtime.cancel_check is not None:
        await runtime.cancel_check()


async def _report_progress(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> None:
    """Emit bounded row progress after the batch commits."""
    if context.runtime.progress_callback is None:
        return
    await context.runtime.progress_callback(
        {
            "phase": f"rehydrating {context.resource_type}",
            "unit": "rows",
            "done": checkpoint.input_count,
            "total": context.expected_count,
            "pct": round(
                100 * checkpoint.input_count / max(context.expected_count, 1), 2
            ),
        }
    )


def _validate_payload(
    model: type,
    resource_id: str,
    stored_hash: str,
    mapped_payload: Any,
) -> str | None:
    """Return a stable reason when a retained mapped payload is unsafe."""
    if (
        not resource_id
        or not isinstance(mapped_payload, dict)
        or _payload_hash(mapped_payload) != stored_hash
    ):
        return "payload_hash_mismatch"
    reserved_fields = {"source_id", "last_seen_run_id", "observed_at", "updated_at"}
    if mapped_payload.get("resource_id") != resource_id or reserved_fields & set(
        mapped_payload
    ):
        return "payload_provenance_invalid"
    column_by_name = {column.name: column for column in model.__table__.columns}
    if set(mapped_payload) - set(column_by_name):
        return "payload_unknown_field"
    for field_name, field_value in mapped_payload.items():
        column_type = column_by_name[field_name].type
        if isinstance(column_type, sa_types.String) and field_value is not None:
            if not isinstance(field_value, str):
                return "payload_column_type_invalid"
        if isinstance(column_type, sa_types.Boolean) and field_value is not None:
            if not isinstance(field_value, bool):
                return "payload_column_type_invalid"
    return None
