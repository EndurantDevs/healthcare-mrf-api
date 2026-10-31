# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish existing ordinary serving candidates in one original Builder transaction."""

import asyncio
import importlib
from contextlib import asynccontextmanager, nullcontext
from dataclasses import asdict, replace
from uuid import UUID

from sqlalchemy import text

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_common_serving_journal as journal
from process import provider_directory_ordinary_row_retention as retention
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.entity_address_candidate_preparation import publish_prepared_entity_address_generation
from process.entity_address_serving_receipt import ordinary_address_receipt_callbacks
from process.native_publication_read_access import preserve_publication_reads
from process.provider_directory_artifact_bundle_preparation import apply_prepared_artifact_bundle
from process.provider_directory_generic_source_artifacts import ordinary_terminal_fence
from process.provider_directory_ordinary_address_admission import finish_profile_cutover
from process.provider_directory_ordinary_facility_inputs import lock_ordinary_input_dependencies
from process.provider_directory_ordinary_initial_retention import initial_retention_scope
from process.provider_directory_output_snapshot_catalog import decoded
from process.provider_directory_owned_wal_transaction import registry_owned_wal_transaction
from process.provider_directory_profile_serving_receipt import ordinary_profile_receipt_continuity
from process.provider_directory_projection_stage import _copy_driver
from process.uhc_flex_practitioner_async_safety import drain_operation

_PAIR_FIELDS = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id")


def _identity(prepared, run_id):
    fhir = prepared.fhir
    validated = fhir._validated_admission_run_id(run_id, run_id, prepared.execution)
    worker_uuid = str(UUID(hex=validated[4:]))
    return worker_uuid, journal.documents.common_generation(worker_uuid)


def _source_pin(prepared):
    """Name one genuine selected Source; the document binds the full qualified selection."""
    receipts.require(bool(prepared.fence.datasets), "common_source_missing")
    requested_ids = set(prepared.nonprofile_admission.captured_inputs.scope_payload["requested_source_ids"])
    selected_pins = [pin for pin in prepared.fence.datasets if pin.source_id in requested_ids]
    pin = min(selected_pins or prepared.fence.datasets, key=lambda item: item.source_id)
    return {
        "source_id": pin.source_id,
        "endpoint_id": pin.endpoint_id,
        "dataset_id": pin.dataset_id,
        "dataset_hash": pin.dataset_hash,
        "parent_run_id": pin.evidence_run_id,
    }


async def _initial_receipt(prepared):
    fhir = prepared.fhir
    build = fhir.profile_initial.build_from_stages(fhir, prepared.profile_bundle.stages, prepared.profile_delta)
    if build is None:
        return None
    receipt = await fhir.profile_initial._committed_receipt(
        fhir, fhir._schema(), fhir._provider_directory_profile_build_id(build)
    )
    receipts.require(receipt is not None, "initial_receipt_missing")
    return {key: receipt[key] for key in ("build_id", "generation_id", "run_id", "payload_sha256")}


async def _lock_publication(prepared):
    """Fence writers and semantic inputs before their current READ COMMITTED comparison."""
    fhir = prepared.fhir
    stages = tuple(prepared.profile_bundle.stages)
    if stages or prepared.profile_delta is not None:
        _schema, _names, lock_timeout, statement_timeout = fhir._provider_directory_artifact_bundle_context(
            fhir._ordered_provider_directory_artifact_bundle(stages), prepared.profile_delta
        )
    else:
        lock_timeout = fhir.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT
        statement_timeout = fhir.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT
    await fhir.profile_initial.lock_metadata(fhir, stages, prepared.profile_delta)
    receipts.require(not prepared.fence.promotion_datasets, "common_source_publication_required")
    await lock_ordinary_input_dependencies(fhir, prepared.nonprofile_admission.captured_inputs)
    await receipts.lock_metadata(fhir.db, fhir._schema())
    await retention.recheck_retained_generation(prepared)
    # This first local cut check still sees the old Profile predecessor.
    await prepared.assert_ready(cutover=True)
    await fhir._configure_provider_directory_artifact_promotion(lock_timeout, statement_timeout)
    return lock_timeout, statement_timeout


async def _apply_profile(prepared, before, cutover_timeout, lock_timeout, statement_timeout):
    """Complete Profile and its last WAL check before the first Address/Overlay write."""
    fhir = prepared.fhir
    stages = tuple(prepared.profile_bundle.stages)

    async def preserve_reads():
        """Carry the incumbent reader grants to the actual initial replacement heaps."""
        session = fhir.db._transaction_binding().session
        driver = await _copy_driver(session)
        for stage in stages:
            await preserve_publication_reads(
                driver, schema=stage.schema, live_table=stage.target_relation, stage_table=stage.stage_table
            )

    continuity = (
        ordinary_profile_receipt_continuity(
            fhir, fhir._schema(), prepared.profile_delta, lock_timeout, statement_timeout
        )
        if stages or prepared.profile_delta is not None
        else nullcontext()
    )
    with initial_retention_scope(prepared, before["roots"]):
        async with continuity:
            forecast = await apply_prepared_artifact_bundle(
                fhir,
                stages,
                profile_delta=prepared.profile_delta,
                cutover_timeout=None if prepared.profile_delta is not None else cutover_timeout,
                settings_configured=True,
                before_swaps=preserve_reads,
            )
    if forecast is not None:
        await fhir._validate_profile_delta_total_wal(fhir._provider_directory_profile_capacity_admission(), forecast)
    await fhir.db.status("SET CONSTRAINTS ALL IMMEDIATE")
    await fhir._profile_capacity_remaining_ms(fhir._provider_directory_profile_capacity_admission())


async def _apply_overlay(prepared):
    """Replace the complete affected Source slice while retaining live heaps and indexes."""
    fhir, schema = prepared.fhir, prepared.fhir._schema()
    capture = prepared.nonprofile_admission.captured_inputs
    stages = [
        stage
        for stage in prepared.nonprofile_bundle.stages
        if stage.target_relation == "provider_directory_address_overlay"
    ]
    receipts.require(len(stages) == 1, "common_overlay_stage_missing")
    stage = stages[0]
    columns = ",".join(fhir._provider_directory_address_overlay_columns())
    await fhir.db.status(
        f"DELETE FROM {receipts.qualified(schema, stage.target_relation)} "
        "WHERE source_id=ANY(CAST(:source_ids AS varchar[]))",
        source_ids=capture.affected_scope["source_ids"],
    )
    await fhir.db.status(
        f"INSERT INTO {receipts.qualified(schema, stage.target_relation)} ({columns}) "
        f"SELECT {columns} FROM {receipts.qualified(schema, stage.stage_table)}"
    )
    await fhir._record_address_alias_artifact_generation(stage)


@asynccontextmanager
async def _address_meter(prepared):
    """Keep Address/journal writes outside the finished Profile mutation windows."""
    fhir = prepared.fhir
    capacity_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    admission_token = preparation._ACTIVE.set(prepared.nonprofile_admission)
    try:
        yield
    finally:
        preparation._ACTIVE.reset(admission_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)


async def _append_publication(prepared, before, expected_postimages, worker_uuid, generation):
    fhir = prepared.fhir
    capture = prepared.nonprofile_admission.captured_inputs
    after = await journal.capture_state(fhir)
    serving = fhir._profile_serving_state_from_row(after["metadata"]["profile"])
    receipts.require(
        0 < serving.control_generation <= prepared.execution.generation
        and serving.selection_proof_id == prepared.execution.attestation.proof_id,
        "common_profile_selection_changed",
    )
    profile_result = fhir.profile_selection_result(
        replace(prepared.execution, generation=serving.control_generation),
        profile_generation_id=serving.generation_id,
        profile_as_of=serving.profile_as_of,
        profile_rows=serving.profile_rows,
        profile_source_evidence_rows=serving.evidence_rows,
    )
    if serving.control_generation != prepared.execution.generation:
        receipts.require(
            not prepared.profile_bundle.stages and prepared.profile_delta is None,
            "common_profile_generation_changed",
        )
        profile_result = await _historical_profile_result(fhir, profile_result)
    document_by_field = {
        "contract_id": journal.documents.CONTRACT,
        "retention": prepared.retained_generation.payload,
        "expected_postimages": expected_postimages,
        "profile_result": profile_result,
        "selection_fingerprint": prepared.execution.attestation.selection_fingerprint,
        "input_hash": prepared.nonprofile_admission.plan.native_address_input_hash,
        "run_uuid": worker_uuid,
        "semantic_inputs": capture.semantic_inputs,
        "qualified_source_ids": sorted(capture.affected_scope["source_ids"]),
        "initial_receipt": await _initial_receipt(prepared),
    }
    await recovery._append(
        fhir.db,
        fhir._schema(),
        generation,
        "published",
        _source_pin(prepared),
        before,
        after,
        preparation=document_by_field,
    )
    history = await recovery._load_history(fhir.db, fhir._schema(), generation)
    return history["published"], asdict(serving)


async def _historical_profile_result(fhir, expected):
    """Keep the genuine prior result when only Address inputs required a new common cut."""
    rows = await fhir.db.all(
        f"SELECT metrics::jsonb -> :metric AS result FROM {fhir._unscoped_qt(fhir._schema(), fhir.ImportRun.__tablename__)} "
        "WHERE importer='provider-directory-fhir' AND status='succeeded' AND finished_at IS NOT NULL "
        "AND metrics::jsonb -> :metric ->> 'profile_generation_id'=:generation",
        metric=fhir.PROFILE_SELECTION_RESULT_METRIC,
        generation=expected["profile_generation_id"],
    )
    results = [
        journal.documents._profile_result(decoded(fhir._pagination_checkpoint_row_mapping(row)["result"]))
        for row in rows
    ]
    receipts.require(bool(results) and all(result == expected for result in results), "common_profile_history_changed")
    return results[0]


async def _apply_publication(prepared, expected_postimages, worker_uuid, generation, cutover_timeout):
    fhir = prepared.fhir
    lock_timeout, statement_timeout = await _lock_publication(prepared)
    before = await journal.capture_state(
        fhir, initial_targets=prepared.nonprofile_admission.captured_inputs.initial_targets
    )
    await _apply_profile(prepared, before, cutover_timeout, lock_timeout, statement_timeout)
    await finish_profile_cutover(prepared)
    async with _address_meter(prepared):
        await _apply_overlay(prepared)
        native = importlib.import_module("process.entity_address_unified")
        callbacks = ordinary_address_receipt_callbacks(fhir.db, fhir._schema(), native._sql_literal)
        await publish_prepared_entity_address_generation(prepared.address, callbacks=callbacks)
        event, serving = await _append_publication(prepared, before, expected_postimages, worker_uuid, generation)
        await fhir.db.status("SET CONSTRAINTS ALL IMMEDIATE")
        await prepared.nonprofile_admission.assert_cutover_complete()
    return event, serving


async def commit_ordinary_serving(prepared, expected_postimages, *, run_id):
    """Keep the genuine commit outcome and prove its immutable event before consuming stages."""
    fhir = prepared.fhir
    receipts.require(fhir.db._transaction_binding() is None, "common_owned_transaction_required")
    receipts.require(set(expected_postimages) == set(journal.documents.FAMILIES), "common_postimages_incomplete")
    worker_uuid, generation = _identity(prepared, run_id)
    groups, outcome, event, serving, failure = [], None, None, None, None
    timeout_seconds = await preparation.remaining_build_seconds(fhir, prepared.nonprofile_admission)
    try:
        async with (
            prepared.nonprofile_admission.publication(fhir, fhir._schema()),
            asyncio.timeout(timeout_seconds) as cutover_timeout,
            registry_owned_wal_transaction(fhir.db) as outcome,
        ):
            await outcome.session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            async with (
                ordinary_terminal_fence(fhir, prepared.nonprofile_admission),
                fhir.profile_control_custody.transaction(
                    fhir,
                    nullcontext(),
                    identity=("artifact_cutover", "ordinary_common", generation),
                    enabled=True,
                    groups=groups,
                ),
            ):
                event, serving = await _apply_publication(
                    prepared, expected_postimages, worker_uuid, generation, cutover_timeout
                )
        fhir.profile_control_custody.consume_owned_groups(fhir, outcome, None, groups=groups)
    except BaseException as error:
        failure = error
        outcome = getattr(error, "outcome", outcome)
    if event is None:
        if failure is not None:
            raise failure
        raise RuntimeError("provider_directory_common_commit_unproved")
    verified = await drain_operation(
        _has_finalized_commit(prepared, generation, event, serving), preserve_cancellation=True
    )
    if not verified:
        if failure is not None:
            raise failure
        raise RuntimeError("provider_directory_common_commit_unproved")
    if failure is not None:
        raise failure
    return prepared.metrics


async def _has_finalized_commit(prepared, generation, expected, serving):
    """Consume prepared stages only after their immutable common commit proof is confirmed."""
    async with asyncio.timeout(10):
        event = await journal.committed_event(prepared.fhir, generation, expected["sha256"])
        if event is None:
            return False
        initial = event["document"]["preparation"]["initial_receipt"]
        if initial is not None and not await recovery.assert_initial_common_receipt(
            prepared.fhir,
            prepared.fhir._schema(),
            initial["build_id"],
            initial["generation_id"],
            event["document"]["writer"]["xid"],
        ):
            return False
        prepared.address.committed = True
        prepared.address.context["publication_state"] = "published"
        await prepared.mark_committed(profile_result=serving)
        document = event["document"]
        result = document["preparation"]["profile_result"]
        prepared.metrics["serving_recovery"] = common_result_metrics(event, serving)
        prepared.metrics[prepared.fhir.PROFILE_SELECTION_RESULT_METRIC] = result
        return True


def common_result_metrics(event, serving, *, outcome="published"):
    """Keep the same canonical common receipt for publication and proven retained reuse."""
    receipts.require(outcome in {"published", "reused"}, "common_outcome_invalid")
    document = event["document"]
    result = document["preparation"]["profile_result"]
    return {
        "contract_id": "provider-directory-common-serving-result.v1",
        "phase": "published",
        "outcome": outcome,
        "generation": document["generation"],
        "journal_sha256": event["sha256"],
        "selection_proof_id": result["proof_id"],
        "source_vector_hash": serving["source_vector_hash"],
        "profile_generation_id": result["profile_generation_id"],
        "address": {
            key: document["after"]["metadata"]["address"][key] for key in ("local_lineage_id", "local_generation")
        },
        "qualified_source_ids": document["preparation"]["qualified_source_ids"],
        "source_pairs": sorted([pair[field] for field in _PAIR_FIELDS] for pair in result["pairs"]),
    }
