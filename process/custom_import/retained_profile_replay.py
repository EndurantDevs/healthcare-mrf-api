# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Rebuild a storage-profile successor from an already acquired sealed source."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from time import monotonic

from db.models.custom_import import CustomImportCaptureBundle, CustomImportGeneration
from process.custom_import.capture_pending import (
    SEGMENTED_PAYLOAD_CONTRACT,
    PendingCaptureRequest,
    append_pending_parquet_part,
    begin_pending_parquet_bundle,
    mark_pending_parquet_eof,
    seal_pending_parquet_bundle,
)
from process.custom_import.capture_store import (
    _identity,
    _load_parquet_part_metadata,
    load_segmented_cursor_headers,
    open_segmented_cursor_part,
    verify_segmented_stream_metadata,
)
from process.custom_import.definition import canonical_json
from process.custom_import.failure_diagnostics import FailurePhase, failure_identifiers, failure_phase
from process.custom_import.https_candidate import _renew_while_preparing
from process.custom_import.operator import inspect_execution_evidence
from process.custom_import.runner_registry import load_current_pointer
from process.custom_import.runner_types import CandidateRunnerError, CandidateRunResult
from process.custom_import.snowflake_candidate import (
    _reservation_unavailable_result,
    _reserve_bundle_execution,
    _unbound_capture_result,
)
from process.custom_import.snowflake_capture import (
    SnowflakeCaptureResult,
    _acquisition_deadline,
    _finish_capture_failure,
    _validate_decimal_receipts,
    _validate_statement_receipts,
)
from process.custom_import.snowflake_segmented_runner import build_segmented_capture
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding

_REPLAY_CONTRACT = "custom-import/retained-profile-replay/v1"


def _require_profile_only(previous, successor):
    """Keep the entire logical definition and connector contract byte-equivalent."""

    previous_definition, new_definition = previous.definition, successor.definition
    old_document = json.loads(previous_definition.canonical)
    old_document["revision"]["definition"] = new_definition.definition_revision
    old_document["storage_profile"] = new_definition.storage_profile
    old_binding = json.loads(previous.binding.canonical)
    old_binding["definition_sha256"] = new_definition.digest
    successor_binding = json.loads(successor.binding.canonical)
    successor_binding.pop("retained_profile_base", None)
    if (
        (previous.dataset_id, previous.schema_revision_id) != (successor.dataset_id, successor.schema_revision_id)
        or new_definition.refresh_mode != "snapshot"
        or new_definition.storage_profile is None
        or new_definition.storage_profile == previous_definition.storage_profile
        or new_definition.definition_revision <= previous_definition.definition_revision
        or canonical_json(old_document) != new_definition.canonical
        or old_binding != successor_binding
        or successor.binding.processing_policy is None
    ):
        raise CandidateRunnerError("retained replay requires a storage-profile-only successor")


async def _load_origin(session, successor, base, parent_statement):
    """Read the exact current generation and its sealed source under the old binding."""

    pointer = await load_current_pointer(session, successor.dataset_id)
    if pointer is None or (pointer.generation_id, pointer.version) != (base.generation_id, base.pointer_version):
        raise CandidateRunnerError("retained replay base is no longer current")
    generation = await session.get(CustomImportGeneration, pointer.generation_id)
    bundle = await session.get(CustomImportCaptureBundle, generation.capture_bundle_id) if generation else None
    if (
        bundle is None
        or bundle.capture_state != "sealed"
        or bundle.payload_contract != SEGMENTED_PAYLOAD_CONTRACT
        or bytes(bundle.manifest_sha256).hex() != base.capture_manifest_sha256
        or (bundle.dataset_id, bundle.definition_revision_id, bundle.schema_revision_id)
        != (generation.dataset_id, generation.definition_revision_id, generation.schema_revision_id)
    ):
        raise CandidateRunnerError("retained replay source is unavailable")
    previous = await load_snowflake_source_binding(
        session,
        definition_revision_id=bundle.definition_revision_id,
        source_binding_revision_id=bundle.source_binding_revision_id,
    )
    _require_profile_only(previous, successor)
    if bytes(bundle.source_binding_sha256) != previous.source_binding_sha256:
        raise CandidateRunnerError("retained replay source binding has changed")
    original = parent_statement(previous)
    receipts = await _stream_receipts(session, previous, bundle.capture_bundle_id)
    if (
        bytes(bundle.source_request_sha256).hex() != original.request.request_sha256
        or bytes(bundle.statement_sha256).hex() != original.statement_sha256
    ):
        raise CandidateRunnerError("retained replay acquisition evidence differs")
    _validate_statement_receipts(original, bundle.snapshot_token, tuple(receipts.values()))
    _validate_decimal_receipts(original, bundle.snapshot_token, tuple(receipts.values()))
    return previous, bundle, receipts


async def _stream_receipts(session, binding, capture_id):
    """Load verified capture metadata without materializing its payload."""

    owner = _identity(
        dataset_id=binding.dataset_id,
        definition_revision_id=binding.definition_revision_id,
        schema_revision_id=binding.schema_revision_id,
    )
    by_slot, _, _ = await _load_parquet_part_metadata(session, owner, capture_id)
    return by_slot


def _replay_receipt(receipt, origin_by_name, successor_binding_sha256):
    """Point to the original acquisition and bind the copy to the successor."""

    original = json.loads(receipt.canonical_manifest)
    replay_by_name = {
        **original,
        "source_binding_sha256": successor_binding_sha256.hex(),
        "retained_replay": {**origin_by_name, "parent_stream_manifest_sha256": receipt.manifest_sha256},
    }
    canonical = canonical_json(replay_by_name)
    return replace(
        receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    )


async def _copy_capture_parts(session_factory, parent_bundle, pending, capture_id, receipts, deadline):
    """Copy authenticated retained parts in order and seal only after complete EOF."""

    parent_ids_by_name = {
        "capture_bundle_id": parent_bundle.capture_bundle_id,
        "dataset_id": parent_bundle.dataset_id,
        "definition_revision_id": parent_bundle.definition_revision_id,
        "schema_revision_id": parent_bundle.schema_revision_id,
    }
    for slot in sorted(receipts):
        async with session_factory() as reader:
            headers = await load_segmented_cursor_headers(reader, stream_slot=slot, **parent_ids_by_name)
        for ordinal in range(1, headers.part_count + 1):
            # Release the read connection before writing, including with a one-connection pool.
            async with session_factory() as reader:
                async with open_segmented_cursor_part(
                    reader, stream_slot=slot, part_ordinal=ordinal, headers=headers, **parent_ids_by_name
                ) as verified:
                    part = verified
            async with session_factory() as writer, writer.begin():
                await append_pending_parquet_part(
                    writer,
                    request=pending,
                    capture_bundle_id=capture_id,
                    capture=part.capture,
                    ordinal=part.ordinal,
                    record_count=part.record_count,
                    arrow_byte_count=part.arrow_byte_count,
                )
        async with session_factory() as reader:
            await verify_segmented_stream_metadata(reader, stream_slot=slot, deadline=deadline, **parent_ids_by_name)
    async with session_factory() as writer, writer.begin():
        for receipt in receipts.values():
            accounting = json.loads(receipt.canonical_manifest)
            await mark_pending_parquet_eof(
                writer,
                request=pending,
                capture_bundle_id=capture_id,
                stream_id=receipt.stream_id,
                part_count=accounting["part_count"],
                record_count=accounting["record_count"],
            )
        await seal_pending_parquet_bundle(
            writer, request=pending, capture_bundle_id=capture_id, receipts=tuple(receipts.values())
        )


async def _retained_capture(session_factory, request, parent_bundle, grant, identity, receipts, capture_id):
    """Use an exact completed replay or create this fence's own capture."""

    async with _renew_while_preparing(session_factory, request, grant):
        if capture_id is not None:
            async with session_factory() as session:
                existing = await _stream_receipts(session, request, capture_id)
            if existing != receipts:
                raise CandidateRunnerError("sealed retained replay has changed")
            return SnowflakeCaptureResult("capture_bound", grant.execution_id, capture_id, grant.fence)
        pending = PendingCaptureRequest(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=grant.execution_id,
            fence=grant.fence,
            token=request.lease_token.encode("utf-8") if isinstance(request.lease_token, str) else request.lease_token,
            request_identity_sha256=identity,
            source_request_sha256=parent_bundle.source_request_sha256,
            statement_sha256=parent_bundle.statement_sha256,
            source_snapshot_token=parent_bundle.snapshot_token,
            policy=request.bundle_request.processing_policy.capture,
            source_binding_revision_id=request.source_binding_revision_id,
            source_binding_sha256=request.source_binding_sha256,
        )
        async with asyncio.timeout(pending.policy.acquisition_deadline_seconds):
            deadline = await _acquisition_deadline(session_factory, request, grant, pending.policy)
            async with asyncio.timeout(deadline - monotonic()):
                async with session_factory() as session, session.begin():
                    capture = await begin_pending_parquet_bundle(session, request=pending)
                await _copy_capture_parts(
                    session_factory, parent_bundle, pending, capture.capture_bundle_id, receipts, deadline
                )
    return SnowflakeCaptureResult("capture_sealed", grant.execution_id, capture.capture_bundle_id, grant.fence)


async def _recover_replay_result(session_factory, dataset_id, execution_id):
    """Recover a committed unpublished result without acquiring or writing again."""

    async with session_factory() as session, session.begin():
        evidence = await inspect_execution_evidence(session, dataset_id=dataset_id, execution_id=execution_id)
    candidate = evidence.generation
    if candidate is None or candidate.publication_state != "sealed_unpublished" or candidate.seal is None:
        raise CandidateRunnerError("completed retained replay has no unpublished sealed candidate")
    return CandidateRunResult(
        status="sealed_unpublished", execution_id=execution_id, generation_id=candidate.generation_id
    )


def _origin_document(previous, parent_bundle, base):
    return {
        "contract": _REPLAY_CONTRACT,
        "parent_generation_id": base.generation_id,
        "parent_pointer_version": base.pointer_version,
        "parent_capture_bundle_id": parent_bundle.capture_bundle_id,
        "parent_capture_manifest_sha256": base.capture_manifest_sha256,
        "parent_source_binding_sha256": previous.source_binding_sha256.hex(),
    }


def _request_identity(origin_by_name, successor_digest):
    request_by_name = {**origin_by_name, "source_binding_sha256": successor_digest.hex()}
    return hashlib.sha256(canonical_json(request_by_name).encode("utf-8")).digest()


async def retained_profile_request_identity(session, successor, parent_statement):
    """Require resume to match the original retained replay reservation."""

    base = successor.binding.retained_profile_base
    previous, parent_bundle, _ = await _load_origin(session, successor, base, parent_statement)
    return _request_identity(_origin_document(previous, parent_bundle, base), successor.source_binding_sha256)


async def run_retained_profile_replay(
    session_factory, successor, request, statement, *, base, parent_statement, writer_transports=None
):
    """Reserve a replay and build the verified successor. Leave publication explicit."""

    policy = successor.binding.processing_policy
    if base is None or base != successor.binding.retained_profile_base:
        raise CandidateRunnerError("retained replay base differs from the approved source binding")
    if writer_transports is not None:
        writer_transports.require_candidate_binding(request)
    async with session_factory() as session:
        previous, parent_bundle, receipts = await _load_origin(session, successor, base, parent_statement)
    origin_by_name = _origin_document(previous, parent_bundle, base)
    identity = _request_identity(origin_by_name, request.source_binding_sha256)
    copies_by_slot = {
        slot: _replay_receipt(receipt, origin_by_name, request.source_binding_sha256)
        for slot, receipt in receipts.items()
    }
    submission, grant = await _reserve_bundle_execution(
        session_factory, request, statement, identity, processing_policy=policy
    )
    failure_identifiers(execution_id=submission.execution_id)
    if grant is None:
        if submission.state == "completed":
            return await _recover_replay_result(session_factory, request.dataset_id, submission.execution_id)
        return CandidateRunResult(status="not_claimed", execution_id=submission.execution_id)
    if grant.state != "running":
        return await _reservation_unavailable_result(session_factory, request, grant)
    if not submission.created and submission.capture_bundle_id is None:
        return await _unbound_capture_result(session_factory, request, grant)
    try:
        with failure_phase(FailurePhase.CAPTURE):
            capture = await _retained_capture(
                session_factory, request, parent_bundle, grant, identity, copies_by_slot, submission.capture_bundle_id
            )
    except BaseException as failure:
        await _finish_capture_failure(session_factory, request, grant, failure)
        raise
    return await build_segmented_capture(
        session_factory,
        request,
        capture,
        policy,
        writer_transports=writer_transports,
        retained_base=(base.generation_id, base.pointer_version),
    )
