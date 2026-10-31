# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recover one common serving cut using a fresh standalone capacity admission."""

import asyncio
import contextvars
import importlib

from sqlalchemy import text

from process import provider_directory_common_serving_journal as journal
from process import provider_directory_common_serving_recovery_metadata as metadata
from process import provider_directory_common_serving_recovery_observation as observation
from process import provider_directory_serving_inverse_capacity as capacity
from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_inputs as inputs
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_cms_storage_continuation import configured_storage_continuation
from process.provider_directory_output_snapshot_catalog import decoded, encoded
from process.uhc_flex_practitioner_async_safety import drain_operation


async def validate_consumed_inverse(database, schema, history):
    """Authenticate the recovered-only run/operation/lease link from immutable signed bytes."""
    fhir = importlib.import_module("process.provider_directory_fhir")
    contract.require(fhir.db is database and fhir._schema() == schema, "database_changed")
    published = history["published"]
    recovered = history["recovered"]["document"]
    lease = await capacity.consumed_inverse_lease(
        fhir, recovered["inverse"], generation=recovered["generation"], published_event_sha256=published["sha256"]
    )
    scope = lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]["scope"]
    contract.require(
        recovered["writer"]["owner"] == scope["owner_oid"]
        and recovered["writer"]["database_oid"] == scope["database_oid"]
        and inputs.document_sha256(recovered["before"]) == scope["forward_state_sha256"]
        and inputs.document_sha256(recovered["preparation"]["retention"]) == scope["retention_payload_sha256"]
        and recovered["preparation"]["qualified_source_ids"] == scope["qualified_source_ids"],
        "inverse_scope_changed",
    )
    return lease


def _outcome(event, lease):
    document = event["document"]
    address = document["after"]["metadata"]["address"]
    profile = document["after"]["metadata"]["profile"]
    result_by_field = dict(
        contract_id=contract.RESULT,
        generation=document["generation"],
        phase="recovered",
        outcome="recovered",
        published_event_sha256=document["predecessor"],
        journal_sha256=event["sha256"],
        inverse=document["inverse"],
        capacity={key: getattr(lease, key) for key in contract.FIELDS["result_capacity"]},
        qualified_source_ids=document["preparation"]["qualified_source_ids"],
        restored_profile_generation_id=None if profile is None else profile["generation_id"],
        address={key: address[key] for key in contract.FIELDS["result_address"]},
    )
    contract.exact(result_by_field, "result")
    return {"serving_recovery": result_by_field}


async def _readback(fhir, task):
    """Settle only this exact committed inverse; pre-consumption is never a success witness."""
    from process import provider_directory_common_initial_witness as witness

    request = capacity.task_execution(task)
    async with witness._read_transaction(fhir) as context:
        history = await inputs.read_common_history(context, request.recovery_execution["generation"])
        if "recovered" not in history:
            return None
        published, event = history["published"], history["recovered"]
        document = event["document"]
        contract.require(
            published["sha256"] == request.recovery_execution["published_event_sha256"]
            and task["source_ids"] == [document["source"]["source_id"]],
            "operation_changed",
        )
        inverse = document["inverse"]
        contract.require(
            inverse["run_id"] == task.get("run_id")
            and inverse["operation_sha256"] == contract.operation_sha256(request.recovery_execution),
            "inverse_run_changed",
        )
        lease = await validate_consumed_inverse(fhir.db, fhir._schema(), history)
        supplied = task[contract.EXECUTION_FIELD]
        contract.require(
            encoded(supplied) == encoded(dict(lease=decoded(lease.canonical_lease_json), signature=lease.signature)),
            "inverse_lease_changed",
        )
        return _outcome(event, lease)


async def _lock_inputs(fhir, document):
    """Pin actual current outputs and immutable copies while ordinary API reads continue."""
    from process.ext import address_alias_sql

    await fhir.db.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
    await receipts.lock_metadata(fhir.db, fhir._schema())
    retained = document["preparation"]["retention"]
    names = set(journal.documents.FAMILIES.values()) | {
        entry["name"] for entry in (*retained["scopes"], *retained["families"])
    }
    await receipts.lock(fhir.db, fhir._schema(), names, mode="SHARE")
    return names


async def _restore_rows(fhir, admission, observed):
    family_by_name = {entry["family"]: entry for entry in admission.geometry["scope"]["families"]}
    for plan in observed.row_restores:
        await admission.boundary()
        deleted = fhir._coerce_rowcount(await fhir.db.status(plan.delete_sql, **dict(plan.parameters)))
        inserted = fhir._coerce_rowcount(await fhir.db.status(plan.insert_sql))
        expected = family_by_name[plan.family]
        contract.require(
            (deleted, inserted) == (expected["live_rows"], expected["insert_rows"]), "restore_rows_changed"
        )
        await admission.boundary()


async def _restore_initial(fhir, admission, observed):
    proofs_by_field = dict(observed.initial_proofs)
    document = observed.document
    schema = fhir._schema()
    for plan in observed.initial_restores:
        await admission.boundary()
        await receipts.retain(fhir.db, schema, {"native": document["after"]["roots"][plan.root]})
        content = decoded(proofs_by_field[plan.family].receipt_json)
        expected_by_field = {
            "native": document["before"]["roots"][plan.root],
            "rows": content["rows"],
            "data_sha256": content["data_sha256"],
        }
        await receipts.restore(fhir.db, schema, expected_by_field, prepared=proofs_by_field[plan.family])
        await admission.boundary()


async def _restore_metadata(fhir, admission, observed):
    from process import entity_address_result_generation as generations
    from process import provider_directory_cms_serving_receipt as cms

    document = observed.document
    schema = fhir._schema()
    session = fhir.db._transaction_binding().session
    for plan in metadata.metadata_restore_sql(schema, document["before"]):
        await session.execute(text(plan.sql), dict(plan.parameters))
        await admission.boundary()
    await recovery._restore_geo(fhir.db, schema, document["before"])
    await admission.boundary()
    await generations.publish_local_entity_address_generation(fhir.db, schema_name=schema)
    await admission.boundary()
    if document["after"]["metadata"]["common_receipt"] is not None:
        native = await cms.capture_native_dependencies(session, schema)
        payload = metadata.common_receipt_payload(document["before"]["metadata"], document["after"]["metadata"], native)
        contract.require(payload is not None, "cms_receipt_changed")
        await cms.append_serving_receipt(session, schema, payload)
        await admission.boundary()


async def _apply(fhir, admission, wall):
    document = admission.captured.document
    await _lock_inputs(fhir, document)
    await admission.boundary()
    observed = await observation.observe_locked_inverse(
        fhir, admission.execution["generation"], source_id=admission.execution["source_ids"][0]
    )
    contract.require(
        observed.event_sha256 == admission.execution["published_event_sha256"] and observed.document == document,
        "event_changed",
    )
    await admission.boundary()
    # Authority transport is bounded preparation work under SHARE locks, never
    # part of the short ACCESS EXCLUSIVE interval or another database owner.
    await admission.refresh("cutover")
    await admission.begin_cutover(wall)
    renamed_names = {name for plan in observed.initial_restores for name in (plan.root, plan.retained_relation)}
    if renamed_names:
        await receipts.lock(fhir.db, fhir._schema(), renamed_names, wait=True)
    await observation.recheck_locked_inverse(observed)
    await admission.boundary()
    await _restore_rows(fhir, admission, observed)
    await _restore_initial(fhir, admission, observed)
    await _restore_metadata(fhir, admission, observed)
    restored = await journal.capture_state(fhir, initial_targets=document["before"]["initial_targets"])
    await recovery._append_document(
        fhir.db,
        fhir._schema(),
        dict(
            generation=observed.generation,
            phase="recovered",
            source=document["source"],
            before=document["after"],
            after=restored,
            preparation=document["preparation"],
            inverse=admission.inverse,
        ),
    )
    await fhir.db.status("SET CONSTRAINTS ALL IMMEDIATE")
    await admission.boundary()


async def _recover_once(fhir, task, control_run_id):
    committed = await _readback(fhir, task)
    if committed is not None:
        return committed
    admission = await capacity.produce_inverse_admission(
        fhir, task, control_run_id=control_run_id, fresh_storage_envelope=configured_storage_continuation()
    )
    async with admission.mutation() as wall:
        await _apply(fhir, admission, wall)
    committed = await _readback(fhir, task)
    contract.require(committed is not None, "commit_unproved")
    return committed


async def _recover(fhir, task, control_run_id):
    """Drain commit/readback through cancellation and preserve the primary failed operation."""
    failure = None
    try:
        return await _recover_once(fhir, task, control_run_id)
    except BaseException as error:
        failure = error
    try:
        committed = await drain_operation(_readback(fhir, task), preserve_cancellation=True)
    except BaseException:
        committed = None
    if committed is not None and not isinstance(failure, asyncio.CancelledError):
        return committed
    raise failure


async def maybe_recover_common(fhir, ctx, task_by_field):
    """Use the existing public recover operation and actual Control-assigned run only."""
    capacity.task_execution(task_by_field)
    contract.require(fhir.db._transaction_binding() is None, "owner_required")
    run = contract.run_handle(ctx.get("control_run_id"))
    contract.require(task_by_field.get("run_id") in {None, run}, "run_owner")
    task_by_field = {**task_by_field, "run_id": run}
    await fhir.ensure_database(bool(ctx.get("context", {}).get("test_mode")))
    result = await asyncio.create_task(_recover(fhir, task_by_field, run), context=contextvars.Context())
    ctx.setdefault("context", {})["audit"] = result
    return result
