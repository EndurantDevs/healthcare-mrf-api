# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact rooted acquisition and native publication in the shared worker."""

from __future__ import annotations

import asyncio
import json
import re
from types import SimpleNamespace

from db.plan_release_storage import storage_relation
from process.control_cancel import raise_if_cancelled
from process.provider_directory_generic_source_policy import (
    source_transaction_available,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime
from process.provider_directory_rooted_graph_store_support import rooted_graph_transaction as source_transaction

ROOTED_GRAPH_FIELD = "rooted_graph"
ROOTED_GRAPH_CONTRACT = "healthporta.provider-directory.rooted-graph-worker.v1"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_ROOT_FIELDS = (
    "root_dataset_id",
    "root_dataset_hash",
    "root_content_proof_sha256",
    "root_cohort_id",
)
_FIELDS = {"contract_id", "operation_key", "publication_acquisition_id", *_ROOT_FIELDS}
ACQUISITION_FLAGS = dict(
    import_resources=True,
    defer_typed_materialization=True,
    full_refresh=True,
    publish_artifacts=False,
    publish_after_acquisition=False,
    publish_corroboration=False,
    publish_source_dataset_only=False,
    probe=False,
    bulk_export=False,
    stale_cleanup=False,
    allow_retained_pagination_reset=False,
)


async def _record_phase(ctx, run_id, operation, state):
    from process.control_lifecycle import mark_control_run

    metrics_by_key = {"rooted_graph": {**operation, "state": state}}
    ctx.setdefault("context", {})["audit"] = metrics_by_key
    recorded = await mark_control_run(
        run_id,
        status="running",
        phase_detail="rooted graph " + state,
        progress_message="rooted graph " + state,
        expected_state=("provider-directory-fhir", "running"),
    )
    if recorded is not True:
        raise ValueError("provider_directory_rooted_graph_worker_attempt_changed")


async def _publication_summary(directory, readiness):
    from api.provider_directory_rooted_fhir_publication import (
        rooted_fhir_publication_summary,
    )

    table = storage_relation(directory._schema(), "provider_directory_endpoint_dataset")
    async with source_transaction(directory.db):
        row = await directory.db.first(
            f"SELECT dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,resource_count,"
            f"status,is_current,published_at FROM {table} WHERE dataset_id=:dataset_id "
            "AND endpoint_id=:endpoint_id AND status='published' AND is_current IS TRUE "
            "AND superseded_at IS NULL",
            dataset_id=readiness.dataset_id,
            endpoint_id=readiness.endpoint_id,
        )
    current_by_field = dict(row._mapping if hasattr(row, "_mapping") else row or {})
    summary_by_field = rooted_fhir_publication_summary(
        SimpleNamespace(**current_by_field, source_ids=(readiness.source_id,)), readiness
    )
    if summary_by_field["state"] not in {"closed", "partial"}:
        raise ValueError("provider_directory_rooted_graph_worker_readback_changed")
    return summary_by_field


def validated_rooted_graph_task(task):
    """Require the exact native source, immutable root and admitted selector."""
    operation = task.get(ROOTED_GRAPH_FIELD)
    if (
        type(operation) is not dict
        or set(operation) != _FIELDS
        or operation.get("contract_id") != ROOTED_GRAPH_CONTRACT
        or any(type(operation.get(name)) is not str for name in _FIELDS)
        or any(
            _HASH.fullmatch(operation[name]) is None
            for name in (
                "operation_key",
                "root_dataset_hash",
                "root_content_proof_sha256",
            )
        )
        or re.fullmatch(r"(?:pdufpd_|pdrgpd_)[0-9a-f]{48}", operation["root_dataset_id"]) is None
        or re.fullmatch(r"pdufc_[0-9a-f]{48}", operation["root_cohort_id"]) is None
        or re.fullmatch(r"pdrga_[0-9a-f]{48}", operation["publication_acquisition_id"]) is None
        or task.get("source_ids") != [PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID]
        or any(task.get(name) is not expected for name, expected in ACQUISITION_FLAGS.items())
        or any(
            task.get(name)
            for name in (
                "seed_only",
                "publish_artifacts_only",
                "dataset_followup_only",
                "dataset_rehydrate_only",
                "canonical_backfill_only",
                "contact_backfill_only",
                "publish_artifacts_targets",
                "test",
                "test_mode",
                "validated_publication_candidate",
                "provider_directory_serving_generation",
                "provider_directory_serving_operation",
                "provider_directory_acquisition_strategy",
            )
        )
    ):
        raise ValueError("provider_directory_rooted_graph_worker_task_invalid")
    return dict(operation)


async def _acquire_pinned_root(directory, operation_by_field):
    from process.provider_directory_rooted_graph_acquisition_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS,
    )
    from process.provider_directory_rooted_graph_operator import (
        _AcquisitionControls,
        _select_exact_current_root,
        acquire_pinned_root_operation,
    )

    current = await _select_exact_current_root(directory.db)
    expected_root_by_field = {
        "root_dataset_id": current.dataset_id,
        "root_dataset_hash": current.dataset_hash,
        "root_content_proof_sha256": current.root_content_proof_sha256,
        "root_cohort_id": current.root_cohort_id,
    }
    if any(operation_by_field[name] != expected for name, expected in expected_root_by_field.items()):
        raise ValueError("provider_directory_rooted_graph_worker_root_changed")
    acquisition_by_field = json.loads(
        await acquire_pinned_root_operation(
            operation_key=operation_by_field["operation_key"],
            expected_publication_acquisition_id=operation_by_field["publication_acquisition_id"],
            controls=_AcquisitionControls(
                concurrency=PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY,
                max_attempts=PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS,
                lease_seconds=120,
                retry_base_seconds=1.0,
                max_retry_seconds=60.0,
                root_timeout_seconds=PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS,
            ),
            database=directory.db,
        )
    )
    if any(
        acquisition_by_field.get(name) != operation_by_field[name]
        for name in ("publication_acquisition_id", "root_dataset_id", "root_dataset_hash")
    ):
        raise ValueError("provider_directory_rooted_graph_worker_acquisition_changed")


async def _ensure_admission(directory, operation_by_field):
    from process.provider_directory_rooted_graph_store_support import table_ref

    table = table_ref("provider_directory_rooted_graph_twin_admission")
    async with source_transaction(directory.db):
        admitted = await directory.db.first(
            f"SELECT acquisition_operation_key,root_dataset_id,root_dataset_hash,root_content_proof_sha256,"
            f"root_cohort_id FROM {table} WHERE publication_acquisition_id=:selector",
            selector=operation_by_field["publication_acquisition_id"],
        )
    if admitted is None:
        await _acquire_pinned_root(directory, operation_by_field)
    elif dict(admitted._mapping if hasattr(admitted, "_mapping") else admitted) != {
        "acquisition_operation_key": operation_by_field["operation_key"],
        **{name: operation_by_field[name] for name in _ROOT_FIELDS},
    }:
        raise ValueError("provider_directory_rooted_graph_worker_admission_changed")


async def _publication_result(directory, operation_by_field, publication):
    readiness = publication.readiness
    if any(
        getattr(readiness, name) != operation_by_field[name] for name in ("publication_acquisition_id", *_ROOT_FIELDS)
    ):
        raise ValueError("provider_directory_rooted_graph_worker_publication_changed")
    metrics_by_key = {
        "rooted_graph": {
            **operation_by_field,
            "state": "published",
            "dataset_id": readiness.dataset_id,
            "dataset_hash": readiness.dataset_hash,
            "replayed": publication.replayed,
        },
        "resource_rows": readiness.resource_count,
        "cohort_complete": readiness.cohort_complete,
        "rooted_graph_complete": readiness.rooted_graph_complete,
        "endpoint_collection_complete": readiness.endpoint_collection_complete,
        "endpoint_complete": readiness.endpoint_complete,
        "rooted_fhir_publication": await _publication_summary(directory, readiness),
        "dataset_followup": directory._provider_directory_dataset_followup(
            source_id=readiness.source_id,
            endpoint_id=readiness.endpoint_id,
            dataset_id=readiness.dataset_id,
            dataset_hash=readiness.dataset_hash,
            parent_run_id=readiness.acquisition_root_run_id,
        ),
    }
    if readiness.request_failure_coverage is None:
        from process.provider_directory_global_profile_followup_contract import (
            build_provider_directory_global_profile_followup,
        )

        metrics_by_key["profile_followup"] = build_provider_directory_global_profile_followup(
            source_id=readiness.source_id,
            dataset_id=readiness.dataset_id,
            parent_run_id=readiness.acquisition_root_run_id,
        )
    return metrics_by_key


async def _execute_rooted_graph_task(directory, ctx, task, operation_by_field, run_id):
    from process.provider_directory_rooted_graph_publication_store import prepare_admitted_rooted_graph_dataset

    await _record_phase(ctx, run_id, operation_by_field, "acquiring")
    await _ensure_admission(directory, operation_by_field)
    await raise_if_cancelled(ctx, task)
    await _record_phase(ctx, run_id, operation_by_field, "publishing")
    handoff = await prepare_admitted_rooted_graph_dataset(
        operation_by_field["publication_acquisition_id"], database=directory.db
    )
    document = handoff.document
    identity = document["identity"]
    if (
        document.get("contract") != "healthporta.provider-directory.rooted-candidate.v1"
        or document.get("phase") != "ready_for_freeze"
        or identity.get("publication_acquisition_id") != operation_by_field["publication_acquisition_id"]
    ):
        raise ValueError("provider_directory_rooted_graph_worker_handoff_changed")
    metrics_by_key = {
        "rooted_graph": {**operation_by_field, "state": "prepared", "dataset_id": identity["dataset_id"]},
        "source_candidate_handoffs": {
            identity["dataset_id"]: {"document": document, "sha256": handoff.document_sha256}
        },
    }
    ctx.setdefault("context", {})["audit"] = metrics_by_key
    runtime = current_rooted_graph_runtime(directory.db)
    return await runtime.publish_prepared_rooted_graph(ctx, task, metrics_by_key)


async def process_rooted_graph_task(directory, ctx, task):
    """Drain durable work on normal control cancellation before releasing its factory."""
    from process.provider_directory_rooted_graph_acquisition_worker import drain_operation
    from process.registry_cms_source_worker import watch_registry_control_cancel

    operation_by_field = validated_rooted_graph_task(task)
    source_transaction_available()
    runtime = current_rooted_graph_runtime(directory.db)
    if runtime is None:
        raise ValueError("provider_directory_rooted_graph_worker_v2_runtime_required")
    run_id = ctx.get("control_run_id") or task.get("run_id")
    if not run_id or task.get("run_id", run_id) != run_id:
        raise ValueError("provider_directory_rooted_graph_worker_run_invalid")
    if runtime.operation != operation_by_field or runtime.run_id != run_id:
        raise ValueError("provider_directory_rooted_graph_worker_task_changed")
    await raise_if_cancelled(ctx, task)
    execution_task = asyncio.create_task(_execute_rooted_graph_task(directory, ctx, task, operation_by_field, run_id))
    cancellation_task = asyncio.create_task(watch_registry_control_cancel(ctx, task))
    try:
        await asyncio.wait((execution_task, cancellation_task), return_when=asyncio.FIRST_COMPLETED)
        if execution_task.done():
            return await execution_task
        return await cancellation_task
    finally:
        for owned_task in (execution_task, cancellation_task):
            if not owned_task.done():
                owned_task.cancel()
        await drain_operation(
            asyncio.gather(execution_task, cancellation_task, return_exceptions=True), preserve_cancellation=True
        )


async def rooted_graph_acquisition_descriptor(database):
    """Describe current metadata; the admitted worker owns complete content validation."""
    from process.provider_directory_dataset_scoped_publication_contract import (
        ProviderDirectoryDatasetScopedPublicationError,
    )
    from process.provider_directory_rooted_graph_current_preflight import _current_metadata
    from process.provider_directory_rooted_graph_source_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    )

    try:
        async with database.transaction():
            await database.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            current = await _current_metadata(database, lock_rows=False)
    except RuntimeError as error:
        if not isinstance(error, ProviderDirectoryDatasetScopedPublicationError) and str(error) != (
            "provider_directory_rooted_graph_predecessor_invalid"
        ):
            raise
        return None
    if current is None:
        return None
    return {
        "contract_id": ROOTED_GRAPH_CONTRACT,
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "canonical_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "root_dataset_id": current.dataset_id,
        "root_dataset_hash": current.dataset_hash,
        "root_content_proof_sha256": current.root_content_proof_sha256,
        "root_cohort_id": current.root_cohort_id,
    }


async def prepare_rooted_graph_operation(database, request):
    """Derive the worker selector from one exact read-only current-root snapshot."""
    from process.provider_directory_rooted_graph_current_preflight import CurrentRootMissingError, _current_metadata
    from process.provider_directory_rooted_graph_operator import build_rooted_graph_single_root_identity

    if (
        type(request) is not dict
        or set(request) != {"operation_key", *_ROOT_FIELDS}
        or any(type(field_value) is not str for field_value in request.values())
        or _HASH.fullmatch(request["operation_key"]) is None
    ):
        raise ValueError("provider_directory_rooted_graph_operation_request_invalid")
    async with database.transaction():
        await database.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        await database.status("SET LOCAL statement_timeout = '5s'")
        await database.status("SET LOCAL lock_timeout = '1s'")
        try:
            current = await _current_metadata(database, lock_rows=False)
        except CurrentRootMissingError as error:
            raise ValueError("provider_directory_rooted_graph_worker_root_changed") from error
        if current is None:
            raise ValueError("provider_directory_rooted_graph_worker_root_changed")
        current_root_by_field = {
            "root_dataset_id": current.dataset_id,
            "root_dataset_hash": current.dataset_hash,
            "root_content_proof_sha256": current.root_content_proof_sha256,
            "root_cohort_id": current.root_cohort_id,
        }
        if any(request[name] != field_value for name, field_value in current_root_by_field.items()):
            raise ValueError("provider_directory_rooted_graph_worker_root_changed")
        identity = build_rooted_graph_single_root_identity(current, operation_key=request["operation_key"])
    return {
        "contract_id": ROOTED_GRAPH_CONTRACT,
        **request,
        "publication_acquisition_id": identity.candidate.acquisition_id,
    }
