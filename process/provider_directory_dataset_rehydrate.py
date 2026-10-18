# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Restore typed rows from one exact current Provider Directory dataset."""

from __future__ import annotations

import dataclasses
import hashlib
from typing import Any

from process.provider_directory_dataset_rehydrate_payload import _validate_payload
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
    _sync_canonical_payload_hashes,
)
from process.provider_directory_dataset_rehydrate_types import (
    DATASET_RESOURCE_TABLE,
    MAX_BATCH_SIZE,
    DatasetRehydrationError,
    DatasetScope,
    RehydrationCheckpoint,
    RehydrationRequest,
    RehydrationRuntime,
    ResourceContext,
    RetainedBatch,
    _clean_text,
    _record_fields,
    _run_cancel_check,
    _table_ref,
)
from process.provider_directory_dataset_rehydrate_types import (
    DEFAULT_BATCH_SIZE as DEFAULT_BATCH_SIZE,
)


async def rehydrate_current_dataset(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
) -> dict[str, Any]:
    """Restore an exact current dataset without network access."""
    _validate_request(request)
    async with _dataset_guard(runtime, request):
        dataset_scope = await _load_dataset_scope(runtime, request)
        selected_resource_types = _selected_resource_types(runtime, request, dataset_scope)
        digest_before = await _verify_dataset_digest(runtime, dataset_scope)
        summary_by_resource = await _rehydrate_selected_resources(
            runtime, request, dataset_scope, selected_resource_types
        )
        digest_after = await _verify_dataset_digest(runtime, dataset_scope)
        await _assert_scope_unchanged(runtime, request, dataset_scope)
    return _execution_summary(request, dataset_scope, summary_by_resource, digest_before, digest_after)


def _validate_request(request: RehydrationRequest) -> None:
    """Reject incomplete identity and unbounded batch options."""
    identity_parts = (
        request.source_id,
        request.dataset_id,
        request.acquisition_root_run_id,
        request.owner_run_id,
    )
    if any(not _clean_text(identity_part) for identity_part in identity_parts):
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_identity_required")
    if not 1 <= request.batch_size <= MAX_BATCH_SIZE:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_batch_size_invalid")


async def _rehydrate_selected_resources(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    scope: DatasetScope,
    selected_resource_types: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    """Process selected resource types serially under the dataset guard."""
    summary_by_resource: dict[str, dict[str, Any]] = {}
    for resource_type in selected_resource_types:
        resource_context = await _resource_context(runtime, request, scope, resource_type)
        summary_by_resource[resource_type] = await _rehydrate_resource(resource_context)
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
        return stored_checkpoint, _resource_summary(stored_checkpoint, resource_proof, reused=True)
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
        await _run_cancel_check(context.runtime)
        current_checkpoint = await _process_one_batch(context, current_checkpoint)
        await _report_progress(context, current_checkpoint)
        if current_checkpoint.state == "rejected":
            raise DatasetRehydrationError("provider_directory_dataset_rehydrate_payload_rejected")
    return current_checkpoint


async def _process_one_batch(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> RehydrationCheckpoint:
    """Write rows and checkpoint progress in one database transaction."""
    async with context.runtime.database.transaction():
        await _assert_scope_unchanged(context.runtime, context.request, context.scope, lock=True)
        retained_records = await _read_retained_batch(context, checkpoint)
        retained_batch = _map_retained_batch(
            context.model,
            retained_records,
            context.scope,
            context.resource_type,
        )
        next_checkpoint = _advance_checkpoint(checkpoint, retained_batch, mapped_count=0)
        if retained_batch.rejection_reasons:
            await _save_checkpoint(context, next_checkpoint)
            return next_checkpoint
        mapped_count = await context.runtime.upsert_batch(
            context.model,
            retained_batch.typed_rows,
            context.scope,
        )
        if mapped_count != len(retained_batch.typed_rows):
            raise DatasetRehydrationError("provider_directory_dataset_rehydrate_upsert_count_mismatch")
        await _sync_canonical_payload_hashes(
            context,
            retained_batch.typed_rows,
        )
        next_checkpoint = _advance_checkpoint(checkpoint, retained_batch, mapped_count=mapped_count)
        await _save_checkpoint(context, next_checkpoint)
    return next_checkpoint


async def _read_retained_batch(
    context: ResourceContext,
    checkpoint: RehydrationCheckpoint,
) -> list[Any]:
    """Read one ordered keyset page of retained mapped payloads."""
    cursor_filter = ""
    query_params_by_name: dict[str, Any] = {
        "dataset_id": context.scope.dataset_id,
        "resource_type": context.resource_type,
        "batch_size": context.request.batch_size,
    }
    if checkpoint.last_resource_id is not None:
        cursor_filter = "AND resource_id > :cursor"
        query_params_by_name["cursor"] = checkpoint.last_resource_id
    retained_records = await context.runtime.database.all(
        f"""SELECT resource_id, payload_hash, payload_json,
                   acquired_resource_sha256
              FROM {_table_ref(context.runtime.schema, DATASET_RESOURCE_TABLE)}
             WHERE dataset_id=:dataset_id AND resource_type=:resource_type
               {cursor_filter}
             ORDER BY resource_id LIMIT :batch_size;""",
        **query_params_by_name,
    )
    if not retained_records:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_input_ended_early")
    return retained_records


def _map_retained_batch(
    model: type,
    retained_records: list[Any],
    scope: DatasetScope,
    resource_type: str,
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
            resource_hash_contract=scope.resource_hash_contract,
            resource_type=resource_type,
            acquired_resource_sha256=retained_fields.get("acquired_resource_sha256"),
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
    last_resource_id = _clean_text(_record_fields(retained_records[-1]).get("resource_id"))
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
    evidence_by_name = {
        **checkpoint.evidence_by_name,
        **(
            {
                "rejection_reasons": sorted(set(retained_batch.rejection_reasons)),
                "rejected_in_batch": rejection_count,
            }
            if rejection_count
            else {}
        ),
    }
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
        await _assert_scope_unchanged(context.runtime, context.request, context.scope, lock=True)
        resource_proof = await _load_resource_proof(context)
        final_state = "complete" if _is_proof_complete(checkpoint, resource_proof) else "proof_failed"
        final_checkpoint = dataclasses.replace(
            checkpoint,
            state=final_state,
            evidence_by_name={**checkpoint.evidence_by_name, "proof": resource_proof.as_dict()},
        )
        await _save_checkpoint(context, final_checkpoint)
    if final_state != "complete":
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_proof_failed")
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
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_scope_changed")


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
            "pct": round(100 * checkpoint.input_count / max(context.expected_count, 1), 2),
        }
    )


async def _verify_pinned_rehydration_digest(directory, runtime, pinned):
    """Hash only the issued immutable child, without mutable parent routing."""
    from process.provider_directory_dataset_rehydrate_scope import _append_digest_rows
    from process.provider_directory_dataset_rehydrate_types import _run_cancel_check

    digest = hashlib.sha256()
    cursor, processed = None, 0
    while True:
        await _run_cancel_check(runtime)
        digest_rows = await pinned.reader.all(
            f"SELECT resource_type, resource_id, payload_hash FROM {pinned.source_relation(directory)} "
            "WHERE dataset_id=:dataset_id AND resource_type NOT LIKE 'LU:%:pass:%' "
            "AND (CAST(:after_type AS varchar) IS NULL OR (resource_type,resource_id)>(:after_type,:after_id)) "
            "ORDER BY resource_type,resource_id LIMIT 10000",
            dataset_id=pinned.scope.dataset_id,
            after_type=cursor[0] if cursor else None,
            after_id=cursor[1] if cursor else None,
        )
        if not digest_rows:
            break
        processed, cursor = _append_digest_rows(digest, digest_rows, processed)
        if len(digest_rows) < 10000:
            break
    if processed != pinned.scope.resource_count or digest.hexdigest() != pinned.scope.dataset_hash:
        raise RuntimeError("provider_directory_dataset_rehydrate_dataset_hash_mismatch")
    return digest.hexdigest()


async def _validate_rehydration_input(directory, runtime, request, pinned, resources):
    """Verify the original digest and selected payloads on the pinned Reader."""
    from process.provider_directory_dataset_rehydrate_types import _run_cancel_check

    digest = await directory._verify_pinned_rehydration_digest(runtime, pinned)
    resource_counts = []
    for resource in resources:
        cursor, count = None, 0
        while True:
            await _run_cancel_check(runtime)
            retained_rows = await pinned.reader.all(
                f"SELECT resource_id,payload_hash,payload_json,acquired_resource_sha256 "
                f"FROM {pinned.source_relation(directory)} "
                "WHERE dataset_id=:dataset_id AND resource_type=:resource_type "
                "AND (CAST(:cursor AS varchar) IS NULL OR resource_id>:cursor) ORDER BY resource_id LIMIT :batch_size",
                dataset_id=request.dataset_id,
                resource_type=resource,
                cursor=cursor,
                batch_size=min(request.batch_size, 5000),
            )
            if not retained_rows:
                break
            batch = _map_retained_batch(runtime.models_by_type[resource], retained_rows, pinned.scope, resource)
            if batch.rejection_reasons:
                raise RuntimeError("provider_directory_dataset_rehydrate_payload_rejected")
            count += batch.input_count
            cursor = batch.last_resource_id
        resource_counts.append((resource, count))
    return digest, tuple(resource_counts)


async def _rehydration_completed_generation(directory, journal, pinned):
    """Read complete, agreeing checkpoint evidence without writing checkpoint rows."""
    from process import provider_directory_dataset_rehydrate_journal as protocol
    from process import provider_directory_dataset_rehydrate_store as store
    from process.provider_directory_dataset_rehydrate_types import ResourceContext
    from process.provider_directory_generic_source_policy import source_transaction

    histories = []
    async with source_transaction(directory.db):
        for resource, expected in journal.expected_by_resource:
            context = ResourceContext(
                journal.runtime,
                journal.request,
                pinned.scope,
                resource,
                directory.RESOURCE_MODELS_BY_TYPE[resource],
                expected,
            )
            checkpoint = await store._load_checkpoint(context)
            if checkpoint is None or checkpoint.state != "complete":
                return None
            proof = await store._load_pinned_resource_proof(context, pinned, directory)
            if checkpoint.expected_count != expected or not store._is_proof_complete(checkpoint, proof):
                return None
            observed = protocol.output_journal(checkpoint.evidence_by_name)
            histories.append(observed)
    return protocol.merge_completed_histories(histories) if histories else None


async def _is_rehydration_output_replay_valid(directory, journal, pinned, operation):
    """Reuse a complete installed cut with one bounded native catalog transaction."""
    from db.plan_release_storage import storage_relation
    from process import provider_directory_dataset_rehydrate_prepare as preparation
    from process import provider_directory_output_snapshot_custody as custody
    from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt

    history = await directory._rehydration_completed_generation(journal, pinned)
    if history is None or history["active"] is not None:
        return False
    output_targets = preparation._targets(directory, journal.members)
    desired_operation_by_field = {**operation, "run_id": None}
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=output_targets) as cap:
        installed = await custody._capture(cap, {target_name: target_name for target_name in output_targets})
        for generation in history["generations"].values():
            intent = generation["operation"]
            if (
                generation["published"] is None
                or intent["kind"] != "rehydrate"
                or {**intent, "run_id": None} != desired_operation_by_field
                or generation["members"] != list(journal.members)
            ):
                continue
            receipts = tuple(
                RetainedOutputReceipt.from_payload(receipt_payload) for receipt_payload in generation["published"]
            )
            if tuple(receipt.target for receipt in receipts) != output_targets:
                raise RuntimeError("provider_directory_dataset_rehydrate_output_set_changed")
            if any(installed[receipt.target].oid != receipt.new_oid for receipt in receipts):
                continue
            target_by_relation = {
                proof.relation: proof.target for receipt in receipts for proof in (receipt.incumbent, receipt.successor)
            }
            await cap.connection.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(journal.runtime.schema, name) for name in sorted(target_by_relation))
                + " IN ACCESS SHARE MODE NOWAIT"
            )
            actual = await custody._capture(cap, target_by_relation)
            if any(
                actual[proof.relation] != proof
                for receipt in receipts
                for proof in (receipt.incumbent, receipt.successor)
            ):
                raise RuntimeError("provider_directory_dataset_rehydrate_native_receipt_changed")
            return True
    return False


async def _rehydrate_prepared_dataset(directory, runtime, request):
    """Prepare isolated outputs and atomically publish their proofs and checkpoints."""
    from process import provider_directory_dataset_rehydrate_prepare as preparation
    from process import provider_directory_dataset_rehydrate_scope as scope_store
    from process.provider_directory_cms_preparation import desired_fence_hash
    from process.provider_directory_dataset_rehydrate_journal_store import (
        CheckpointOutputJournal,
        preparation_operation,
        verify_retry_predecessor,
    )
    from process.provider_directory_dataset_rehydrate_pin import pinned_current_source
    from process.provider_directory_dataset_rehydrate_publish import publish_prepared_rehydration
    from process.provider_directory_dataset_rehydrate_report import _execution_summary

    _validate_request(request)
    predecessor = await verify_retry_predecessor(runtime, request)
    async with scope_store._dataset_guard(runtime, request):
        async with pinned_current_source(runtime, request, directory=directory) as pinned:
            resources = tuple(sorted(scope_store._selected_resource_types(runtime, request, pinned.scope)))
            digest, expected = await directory._validate_rehydration_input(runtime, request, pinned, resources)
            journal = CheckpointOutputJournal(runtime, request, pinned.scope, expected, predecessor)
            fence = await directory._resolve_provider_directory_artifact_datasets([request.source_id])
            operation_by_field = dict(
                kind="rehydrate",
                run_id=request.owner_run_id,
                producer="provider-directory-fhir",
                source_pins=[preparation._source_pin(pinned.scope)],
                resource_types=list(resources),
                source_fence_digest=desired_fence_hash(fence),
            )
            reused = await directory._is_rehydration_output_replay_valid(journal, pinned, operation_by_field)
            if not reused:
                operation_by_field = await preparation_operation(
                    journal, operation_by_field, preparation._targets(directory, resources)
                )
                async with preparation.prepared_compatibility_outputs(
                    directory, pinned, operation_by_field, checkpoint_journal=journal
                ) as prepared:
                    await publish_prepared_rehydration(
                        directory,
                        pinned,
                        operation_by_field,
                        prepared,
                        checkpoint_journal=journal,
                        fence=fence,
                        artifact_bundle=directory.ProviderDirectoryArtifactBundle(),
                    )
            digest_after = await directory._verify_pinned_rehydration_digest(runtime, pinned)
            summaries = await directory._rehydration_output_summaries(journal, pinned, reused)
            return _execution_summary(request, pinned.scope, summaries, digest, digest_after)


async def _rehydration_output_summaries(directory, journal, pinned, reused):
    """Recheck completed output proofs before emitting the existing result contract."""
    from process import provider_directory_dataset_rehydrate_pin as source_pin
    from process import provider_directory_dataset_rehydrate_store as store
    from process.provider_directory_dataset_rehydrate_report import _resource_summary
    from process.provider_directory_dataset_rehydrate_types import ResourceContext
    from process.provider_directory_generic_source_policy import source_transaction

    summary_by_resource = {}
    async with source_transaction(directory.db):
        fresh, _, binding = await source_pin._capture_source(journal.runtime, journal.request)
        if fresh != pinned.scope or binding != pinned.binding(directory):
            raise RuntimeError("provider_directory_dataset_rehydrate_source_binding_changed")
        for resource, expected in journal.expected_by_resource:
            context = ResourceContext(
                journal.runtime,
                journal.request,
                pinned.scope,
                resource,
                directory.RESOURCE_MODELS_BY_TYPE[resource],
                expected,
            )
            checkpoint = await store._load_checkpoint(context)
            proof = await store._load_pinned_resource_proof(context, pinned, directory)
            if (
                checkpoint is None
                or checkpoint.state != "complete"
                or checkpoint.expected_count != expected
                or not store._is_proof_complete(checkpoint, proof)
            ):
                raise RuntimeError("provider_directory_dataset_rehydrate_proof_incomplete")
            summary_by_resource[resource] = _resource_summary(checkpoint, proof, reused=reused)
    return summary_by_resource
