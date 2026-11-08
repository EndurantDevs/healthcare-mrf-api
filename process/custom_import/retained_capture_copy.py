# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Copy sealed segments under a fresh, ordinary pending-capture producer."""

import asyncio
import base64
import hashlib
import json
import time

from sqlalchemy import select

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportExecution,
)
from db.models.custom_import_retained_copy import CustomImportRetainedCaptureCopy as Copy
from process.custom_import import execution as lifecycle
from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy_contract as contract
from process.custom_import import retained_capture_copy_native as native
from process.custom_import.capture_pending import (
    _lock_bundle,
    _pending_stream,
    _verified_part_values,
    append_pending_parquet_part,
    begin_pending_parquet_bundle,
    mark_pending_parquet_eof,
    seal_pending_parquet_bundle,
)
from process.custom_import.capture_store import open_segmented_parquet_parts
from process.custom_import.publication import _capture_source_bundle_digest


def authorize(signed, bearer, configuration, *, now=None):
    """Only this exact profile may execute the closed controller authorization."""
    key = base64.urlsafe_b64decode(configuration["verifying_key"] + "=")
    descriptor = contract.verify(signed, verifying_key=key, bearer=bearer, now=now)
    if (
        descriptor["owner_image"] != configuration["image"]
        or descriptor["owner_profile_sha256"] != hashlib.sha256(custody.canonical(configuration)).hexdigest()
    ):
        custody.unavailable()
    return descriptor


async def prepare(factory, document, configuration):
    """Resolve a compatible old capture without reserving or claiming an execution."""
    if (
        type(document) is not dict
        or document.keys() != {"operation", "locator", "bearer", "capture_secret_uid", "authorization_expires_at"}
        or document["operation"] != "prepare"
    ):
        custody.unavailable()
    locator = document["locator"]
    if type(locator) is not dict or locator.keys() != native.LOCATOR_FIELDS:
        custody.unavailable()
    bearer = custody.checked_bearer(document["bearer"].encode("ascii"))
    async with factory() as session, session.begin():
        value = await native.load_source(session, locator, bearer)
        now = await lifecycle._database_now(session)
        remaining = (custody.timestamp(document["authorization_expires_at"]) - now).total_seconds()
        if not 0 < remaining <= 900:
            custody.unavailable()
    descriptor = native.descriptor_fields(value) | {
        "capture_secret_uid": document["capture_secret_uid"],
        "authorization_expires_at": document["authorization_expires_at"],
        "owner_profile_sha256": hashlib.sha256(custody.canonical(configuration)).hexdigest(),
        "owner_image": configuration["image"],
    }
    return {"descriptor": contract.checked_descriptor(descriptor)}


async def locked_copy(session, signed):
    """Serialize copy metadata by the existing dataset lock and exact authorization."""
    descriptor = contract.checked_descriptor(signed["descriptor"])
    await native.require_control_authority(session)
    await lifecycle._lock_dataset(session, descriptor["dataset_id"])
    row = await session.scalar(
        select(Copy)
        .where(Copy.copy_id == contract.identity(signed))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is not None and (
        row.canonical_authorization != custody.canonical(signed).decode("ascii")
        or row.dataset_id != descriptor["dataset_id"]
        or row.source_capture_bundle_id != descriptor["source_capture_bundle_id"]
        or bytes(row.source_manifest_sha256).hex() != descriptor["source_capture_manifest_sha256"]
    ):
        custody.unavailable()
    return row


async def _reserve(session, signed, copy_input):
    request = copy_input.request
    submission = await lifecycle.reserve_execution(
        session,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        idempotency_key=request.idempotency_key,
        mechanism="local",
        request_identity_sha256=copy_input.identity,
        source_binding_revision_id=request.source_binding_revision_id,
    )
    # A different entry must never be adopted, even if its semantic hashes agree.
    if not submission.created:
        custody.unavailable()
    grant = await lifecycle.claim_execution(session, execution_id=submission.execution_id, token=request.lease_token)
    if grant is None or grant.fence != 1 or grant.state != "running":
        custody.unavailable()
    pending = copy_input.pending(grant.execution_id, grant.fence)
    bundle = await begin_pending_parquet_bundle(session, request=pending)
    source_capture = copy_input.source
    copy_record = Copy(
        copy_id=contract.identity(signed),
        canonical_authorization=custody.canonical(signed).decode("ascii"),
        dataset_id=request.dataset_id,
        execution_id=grant.execution_id,
        fence=grant.fence,
        source_execution_id=source_capture.execution_id,
        source_fence=source_capture.fence,
        source_token_sha256=source_capture.token_sha256,
        source_capture_bundle_id=source_capture.capture_bundle_id,
        source_manifest_sha256=source_capture.manifest_sha256,
        capture_bundle_id=bundle.capture_bundle_id,
        phase="active",
        source_pin_active=True,
    )
    session.add(copy_record)
    await session.flush()
    return copy_record


async def current_copy(session, signed, value):
    """Resume only the same live native producer; never claim an expired attempt."""
    row = await locked_copy(session, signed)
    if row is None or row.phase == "closed":
        custody.unavailable()
    pending = value.pending(row.execution_id, row.fence)
    await _lock_bundle(session, pending, row.capture_bundle_id)
    return row, pending


async def reserve_copy(factory, signed, bearer, configuration):
    """Allocate the native producer and source pin in one transaction, once."""
    descriptor = authorize(signed, bearer, configuration, now=time.time())
    async with factory() as session, session.begin():
        await lifecycle._lock_dataset(session, descriptor["dataset_id"])
        value = await native.load_source(session, descriptor, bearer)
        native.require_descriptor(value, descriptor)
        now = await lifecycle._database_now(session)
        if now >= custody.timestamp(descriptor["authorization_expires_at"]):
            custody.unavailable()
        row = await locked_copy(session, signed)
        if row is None:
            row = await _reserve(session, signed, value)
        else:
            row, _pending = await current_copy(session, signed, value)
        return snapshot(row, (await session.get(CustomImportExecution, row.execution_id)).state)


def snapshot(row, engine_state):
    """Freeze control identity without exposing bearer or retained source bytes."""
    return {
        "copy_id": row.copy_id,
        "engine_execution_id": row.execution_id,
        "capture_bundle_id": row.capture_bundle_id,
        "copy_state": row.phase,
        "engine_state": engine_state,
        "job_uid": row.first_job_uid,
        "terminal_job_state": row.terminal_job_state,
        "pods_stopped": row.pods_stopped_at is not None,
        "launch_sent": row.launch_sent_at is not None,
        "cleaned": row.cleaned_at is not None,
        "root_device": row.root_device,
        "root_inode": row.root_inode,
        "cleanup_started": row.cleanup_started_at is not None,
    }


async def inspect_copy(factory, signed):
    """Observe exact persisted scope without claiming, renewing or creating work."""
    async with factory() as session, session.begin():
        row = await locked_copy(session, signed)
        if row is None:
            return None
        execution = await bound_execution(session, row, signed)
        return snapshot(row, execution.state)


async def _heartbeat(factory, signed, value):
    while True:
        async with factory() as session, session.begin():
            row, _pending = await current_copy(session, signed, value)
            grant = await lifecycle.heartbeat_execution(
                session, execution_id=row.execution_id, fence=row.fence, token=value.request.lease_token
            )
            if grant is None or grant.state != "running":
                custody.unavailable()
        await asyncio.sleep(min(20, lifecycle.DEFAULT_LEASE_SECONDS / 3))


async def _closed_part(session, pending, destination, part):
    values = await _verified_part_values(
        session, pending, part.capture, part.ordinal, part.record_count, part.arrow_byte_count
    )
    bundle, _ = await _lock_bundle(session, pending, destination)
    stream = await _pending_stream(session, bundle, pending, part.receipt.stream_id)
    stored = await session.get(CustomImportCaptureParquetPart, (destination, stream.stream_slot, part.ordinal))
    if stored is None or any(getattr(stored, name) != expected for name, expected in values.items()):
        custody.unavailable()


async def _copy_parts(factory, signed, copy_input):
    async with factory() as session, session.begin():
        copy_record, pending = await current_copy(session, signed, copy_input)
        destination = copy_record.capture_bundle_id
        closed_slots = set(
            (
                await session.scalars(
                    select(CustomImportCapture.stream_slot).where(
                        CustomImportCapture.capture_bundle_id == destination, CustomImportCapture.eof_at.is_not(None)
                    )
                )
            ).all()
        )
    slots_by_stream = dict(copy_input.source.stream_slots)
    async with factory() as reader, reader.begin():
        async with open_segmented_parquet_parts(
            reader,
            capture_bundle_id=copy_input.source.capture_bundle_id,
            dataset_id=pending.dataset_id,
            definition_revision_id=pending.definition_revision_id,
            schema_revision_id=pending.schema_revision_id,
        ) as parts:
            async for part in parts:
                await _retain_part(
                    factory,
                    pending,
                    destination,
                    part,
                    is_closed=slots_by_stream[part.receipt.stream_id] in closed_slots,
                )
    # EOF of the verified iterator, including all original accounting, precedes every final transition.
    async with factory() as session, session.begin():
        copy_record, pending = await current_copy(session, signed, copy_input)
        for receipt in copy_input.source.receipts:
            accounting = json.loads(receipt.canonical_manifest)
            await mark_pending_parquet_eof(
                session,
                request=pending,
                capture_bundle_id=destination,
                stream_id=receipt.stream_id,
                part_count=accounting["part_count"],
                record_count=accounting["record_count"],
            )
        await _seal_copy(session, copy_record, pending, copy_input.source)


async def _seal_copy(session, row, pending, source):
    await seal_pending_parquet_bundle(
        session, request=pending, capture_bundle_id=row.capture_bundle_id, receipts=source.receipts
    )
    target = await session.get(CustomImportCaptureBundle, row.capture_bundle_id, populate_existing=True)
    if (
        target.canonical_manifest != source.canonical_manifest
        or bytes(target.manifest_sha256) != source.manifest_sha256
    ):
        custody.unavailable()
    scope_by_field = {
        "dataset_id": pending.dataset_id,
        "definition_revision_id": pending.definition_revision_id,
        "schema_revision_id": pending.schema_revision_id,
    }
    original = await _capture_source_bundle_digest(
        session, capture_bundle_id=source.capture_bundle_id, **scope_by_field
    )
    copied = await _capture_source_bundle_digest(session, capture_bundle_id=target.capture_bundle_id, **scope_by_field)
    if original != copied:
        custody.unavailable()
    grant = await lifecycle.heartbeat_execution(
        session, execution_id=pending.execution_id, fence=pending.fence, token=pending.token
    )
    if grant is None or grant.state != "running":
        custody.unavailable()
    row.phase, row.source_pin_active = "sealed", False
    await session.flush()


async def copy_capture(factory, signed, bearer, configuration):
    """Run one authorized copy; native authority and the original acquisition deadline bound every part."""
    descriptor = authorize(signed, bearer, configuration, now=time.time())
    state = await reserve_copy(factory, signed, bearer, configuration)
    if state["copy_state"] == "sealed":
        return state
    async with factory() as session, session.begin():
        value = await native.load_source(session, descriptor, bearer)
        native.require_descriptor(value, descriptor)
    async with asyncio.TaskGroup() as tasks:
        heartbeat = tasks.create_task(_heartbeat(factory, signed, value))
        try:
            await _copy_parts(factory, signed, value)
        finally:
            heartbeat.cancel()
    return await inspect_copy(factory, signed)


async def finish_failure(factory, signed, bearer, primary):
    """Use ordinary capture finality after the trusted process has stopped copying."""
    try:
        async with factory() as session, session.begin():
            row = await locked_copy(session, signed)
            if row is None or row.phase != "active":
                return
            state = "canceled" if isinstance(primary, asyncio.CancelledError) else "failed"
            transition = await lifecycle.finish_execution(
                session,
                execution_id=row.execution_id,
                fence=row.fence,
                token=bearer,
                terminal_state=state,
                terminal_reason="source_capture_failed" if state == "failed" else None,
            )
            if transition.state == "canceling" and state == "failed":
                await lifecycle.finish_execution(
                    session, execution_id=row.execution_id, fence=row.fence, token=bearer, terminal_state="canceled"
                )
    except BaseException:
        primary.add_note("Copy finalization is unconfirmed; native custody remains retained.")


async def bound_execution(session, copy_record, signed):
    """Keep native producer and destination custody joined during historical observation."""
    descriptor = signed["descriptor"]
    execution = await session.get(CustomImportExecution, copy_record.execution_id)
    bundle = await session.get(CustomImportCaptureBundle, copy_record.capture_bundle_id)
    expected_by_field = {
        "dataset_id": descriptor["dataset_id"],
        "definition_revision_id": descriptor["definition_revision_id"],
        "schema_revision_id": descriptor["schema_revision_id"],
        "source_binding_revision_id": descriptor["source_binding_revision_id"],
        "request_identity_sha256": bytes.fromhex(descriptor["request_identity_sha256"]),
    }
    if (
        execution is None
        or bundle is None
        or any(
            getattr(execution, name) != expected_value or getattr(bundle, name) != expected_value
            for name, expected_value in expected_by_field.items()
        )
        or (
            execution.idempotency_key != descriptor["controller_execution_id"]
            or execution.capture_bundle_id not in (None, copy_record.capture_bundle_id)
            or bundle.producing_execution_id != copy_record.execution_id
            or bundle.producing_fence != copy_record.fence
            or bytes(bundle.producing_token_sha256).hex() != descriptor["bearer_sha256"]
            or copy_record.phase == "sealed"
            and (execution.capture_bundle_id != copy_record.capture_bundle_id or bundle.capture_state != "sealed")
        )
    ):
        custody.unavailable()
    return execution


async def _retain_part(factory, pending, destination, part, *, is_closed):
    """Keep a whole original segment inside the existing guarded transaction."""
    async with factory() as writer, writer.begin():
        if is_closed:
            await _closed_part(writer, pending, destination, part)
        else:
            await append_pending_parquet_part(
                writer,
                request=pending,
                capture_bundle_id=destination,
                capture=part.capture,
                ordinal=part.ordinal,
                record_count=part.record_count,
                arrow_byte_count=part.arrow_byte_count,
            )
