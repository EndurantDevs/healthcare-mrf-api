# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit same-build writer transfer without rewriting retained provenance."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import re

from sqlalchemy import select

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCaptureBundle,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportGenerationSeal,
    CustomImportLease,
)
from db.models.custom_import_continuation import CustomImportBuildContinuation
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import MAX_LEASE_SECONDS, IdempotencyConflict, lease_token_sha256
from process.custom_import.runner_registry import database_now, load_current_pointer, lock_dataset, lock_lease

CONTRACT = "custom-import/build-continuation/v1"
_HEX = re.compile(r"[0-9a-f]{64}", re.ASCII)
_HASH_FIELDS = (
    "producer_token_sha256",
    "request_identity_sha256",
    "worker_identity_sha256",
    "predecessor_identity_sha256",
    "nonce_sha256",
)
_INTEGER_FIELDS = ("build_id", "snapshot_family_id", "capture_bundle_id", "producer_fence")
_FIELDS = frozenset(
    (
        "contract",
        "authorization_id",
        "engine_execution_id",
        "expected_authority_id",
        "base_generation_id",
        "base_pointer_version",
        "processing_deadline_removed",
        "launch_expires_at",
        *_INTEGER_FIELDS,
        *_HASH_FIELDS,
    )
)


def _is_positive(value):
    return type(value) is int and 0 < value < 2**63


def parse_build_continuation_authorization(document):
    """Reject unknown fields and noncanonical identities before touching storage."""

    if type(document) is not dict or document.keys() != _FIELDS:
        raise ValueError("invalid continuation fields")
    if document["contract"] != CONTRACT or document["processing_deadline_removed"] is not True:
        raise ValueError("invalid continuation contract")
    if any(
        type(document[key]) is not str or _HEX.fullmatch(document[key]) is None
        for key in ("authorization_id", *_HASH_FIELDS)
    ):
        raise ValueError("invalid continuation digest")
    execution_id = document["engine_execution_id"]
    if (
        type(execution_id) is not str
        or not re.fullmatch(r"[1-9][0-9]{0,18}", execution_id, re.ASCII)
        or not _is_positive(int(execution_id))
    ):
        raise ValueError("invalid continuation execution")
    if any(not _is_positive(document[key]) for key in _INTEGER_FIELDS):
        raise ValueError("invalid continuation identity")
    if any(
        document[key] is not None and not _is_positive(document[key])
        for key in ("expected_authority_id", "base_generation_id")
    ):
        raise ValueError("invalid continuation predecessor")
    pointer = document["base_pointer_version"]
    if type(pointer) is not int or not 0 <= pointer < 2**63:
        raise ValueError("invalid continuation pointer")
    if (document["base_generation_id"] is None) != (pointer == 0):
        raise ValueError("invalid continuation base")
    _launch_time(document["launch_expires_at"])
    return dict(document)


def _launch_time(value):
    if type(value) is not str or len(value) > 40:
        raise ValueError("invalid continuation launch window")
    timestamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() != dt.timedelta(0):
        raise ValueError("continuation launch window must be UTC")
    return timestamp


def authorization_bytes(authorization):
    """Encode only the closed authorization mapping for stable acknowledgement."""

    return json.dumps(authorization, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


async def _locked_retained(session, dataset_id, execution_id, build_id):
    """Use the same parent-first lock order as normal processing and finality."""

    await lock_dataset(session, dataset_id)
    execution = (
        await session.scalars(
            select(CustomImportExecution)
            .where(CustomImportExecution.execution_id == execution_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    lease = await lock_lease(session, execution_id)
    build = (
        await session.scalars(
            select(CustomImportBuildAttempt)
            .where(CustomImportBuildAttempt.build_id == build_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if execution is None or lease is None or build is None or build.execution_id != execution_id:
        raise IdempotencyConflict("retained build is unavailable")
    return execution, lease, build


def _same_request(
    execution,
    *,
    dataset_id,
    definition_revision_id,
    schema_revision_id,
    source_binding_revision_id,
    idempotency_key,
    request_identity_sha256,
):
    expected_by_field = locals().copy()
    expected_by_field.pop("execution")
    if any(getattr(execution, key) != value for key, value in expected_by_field.items()):
        raise IdempotencyConflict("retained execution identity differs")


def _same_build(authorization, execution, build, snapshot):
    expected_by_field = {
        "build_id": build.build_id,
        "snapshot_family_id": snapshot.family_id,
        "capture_bundle_id": execution.capture_bundle_id,
        "producer_fence": build.producing_fence,
        "producer_token_sha256": bytes(build.producing_token_sha256).hex(),
        "request_identity_sha256": bytes(execution.request_identity_sha256).hex(),
        "base_generation_id": build.base_generation_id,
        "base_pointer_version": build.base_pointer_version,
    }
    if any(authorization[key] != value for key, value in expected_by_field.items()):
        raise IdempotencyConflict("continuation build identity differs")
    for key in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id", "capture_bundle_id"):
        if getattr(build, key) != getattr(execution, key) or getattr(snapshot, key) != getattr(build, key):
            raise IdempotencyConflict("continuation snapshot identity differs")
    if (
        snapshot.producing_fence != build.producing_fence
        or snapshot.producing_token_sha256 != build.producing_token_sha256
        or build.request_identity_sha256 != execution.request_identity_sha256
    ):
        raise IdempotencyConflict("continuation producer identity differs")


def _eligible(execution, lease, build, snapshot, *, first):
    if (
        execution.state != "running"
        or lease.fence != build.producing_fence
        or build.source_frozen_at is None
        or build.plan_stage != "complete"
        or build.plan_complete_at is None
        or build.candidate_error_count != 0
        or build.phase not in {"graph", "output", "verifying", "verified"}
        or snapshot.generation_id != build.generation_id
    ):
        raise IdempotencyConflict("retained build cannot continue")
    if first and (build.phase != "graph" or build.generation_id is not None or snapshot.frozen_at is not None):
        raise IdempotencyConflict("initial continuation requires an admitted unsealed graph")


async def _snapshot_row(session, authorization, *, lock=True):
    snapshot = await session.get(CustomImportSnapshotFamily, authorization["snapshot_family_id"], with_for_update=lock)
    if snapshot is None:
        raise IdempotencyConflict("retained snapshot is unavailable")
    return snapshot


async def _require_base_and_capture(session, execution, build):
    pointer = await load_current_pointer(session, execution.dataset_id)
    if (None if pointer is None else pointer.generation_id) != build.base_generation_id:
        raise IdempotencyConflict("retained base generation changed")
    if (0 if pointer is None else pointer.version) != build.base_pointer_version:
        raise IdempotencyConflict("retained base pointer changed")
    capture = await session.get(CustomImportCaptureBundle, build.capture_bundle_id)
    if (
        capture is None
        or capture.capture_state != "sealed"
        or capture.sealed_at is None
        or capture.producing_execution_id != execution.execution_id
    ):
        raise IdempotencyConflict("retained capture is not sealed")


async def _require_previous_authority(session, lease, build):
    if lease.continuation_id is None:
        if lease.token_sha256 != build.producing_token_sha256:
            raise IdempotencyConflict("retained producer lease differs")
        return
    previous = await session.get(CustomImportBuildContinuation, lease.continuation_id)
    if (
        previous is None
        or previous.build_id != build.build_id
        or previous.producer_fence != build.producing_fence
        or previous.producer_token_sha256 != build.producing_token_sha256
        or previous.nonce_sha256 != lease.token_sha256
    ):
        raise IdempotencyConflict("previous continuation identity differs")
    if (
        build.generation_id is not None
        and await session.get(CustomImportGenerationSeal, build.generation_id) is not None
    ):
        raise IdempotencyConflict("a sealed generation cannot acquire another writer")


def _authorization_row(document, execution, build):
    values_by_field = {
        key: document[key]
        for key in (
            "authorization_id",
            "expected_authority_id",
            "base_generation_id",
            "base_pointer_version",
            "processing_deadline_removed",
        )
    }
    values_by_field.update({key: document[key] for key in _INTEGER_FIELDS if key != "snapshot_family_id"})
    values_by_field.update({key: bytes.fromhex(document[key]) for key in _HASH_FIELDS})
    encoded = authorization_bytes(document)
    return CustomImportBuildContinuation(
        **values_by_field,
        contract=CONTRACT,
        execution_id=execution.execution_id,
        dataset_id=execution.dataset_id,
        definition_revision_id=execution.definition_revision_id,
        schema_revision_id=execution.schema_revision_id,
        snapshot_family_id=document["snapshot_family_id"],
        original_deadline_at=build.build_deadline_at,
        launch_expires_at=_launch_time(document["launch_expires_at"]),
        canonical_authorization=encoded.decode("utf-8"),
        authorization_sha256=hashlib.sha256(encoded).digest(),
    )


def authorization_receipt(authorization_record):
    """Acknowledge durable authorization without exposing its nonce."""

    return {
        "contract": "custom-import/build-continuation-authorized/v1",
        "continuation_id": authorization_record.continuation_id,
        "authorization_id": authorization_record.authorization_id,
        "authorization_sha256": bytes(authorization_record.authorization_sha256).hex(),
        "build_id": authorization_record.build_id,
        "engine_execution_id": str(authorization_record.execution_id),
        "processing_deadline_removed": True,
    }


async def authorize_build_continuation(session, authorization, **pins):
    """Record an exact transfer and fence the old bearer before starting a worker."""

    document = parse_build_continuation_authorization(authorization)
    execution, lease, build = await _locked_retained(
        session, pins["dataset_id"], int(document["engine_execution_id"]), document["build_id"]
    )
    _same_request(execution, **pins)
    snapshot = await _snapshot_row(session, document)
    _same_build(document, execution, build, snapshot)
    existing = (
        await session.scalars(
            select(CustomImportBuildContinuation).where(
                CustomImportBuildContinuation.authorization_id == document["authorization_id"]
            )
        )
    ).one_or_none()
    if existing is not None:
        if (
            existing.canonical_authorization != authorization_bytes(document).decode("utf-8")
            or lease.continuation_id != existing.continuation_id
        ):
            raise IdempotencyConflict("continuation authorization replay differs")
        return authorization_receipt(existing)
    now = await database_now(session)
    _eligible(execution, lease, build, snapshot, first=lease.continuation_id is None)
    if (
        lease.continuation_id != document["expected_authority_id"]
        or lease.expires_at is None
        or lease.expires_at > now
        or not now < _launch_time(document["launch_expires_at"]) <= now + dt.timedelta(minutes=15)
    ):
        raise IdempotencyConflict("continuation transfer is not admissible")
    await _require_base_and_capture(session, execution, build)
    await _require_previous_authority(session, lease, build)
    authorization_record = _authorization_row(document, execution, build)
    session.add(authorization_record)
    await session.flush()
    lease.continuation_id, lease.continuation_started_at = authorization_record.continuation_id, None
    lease.token_sha256, lease.updated_at = authorization_record.nonce_sha256, now
    await session.flush()
    return authorization_receipt(authorization_record)


async def start_build_continuation(session, authorization, nonce, *, lease_seconds):
    """Consume one launch exactly once; later work uses renewable finite leases."""

    document = parse_build_continuation_authorization(authorization)
    if (
        type(nonce) is not bytes
        or len(nonce) != 32
        or not _is_positive(lease_seconds)
        or lease_seconds > MAX_LEASE_SECONDS
    ):
        raise ValueError("invalid continuation start")
    authorization_record = (
        await session.scalars(
            select(CustomImportBuildContinuation).where(
                CustomImportBuildContinuation.authorization_id == document["authorization_id"]
            )
        )
    ).one_or_none()
    if authorization_record is None or authorization_record.canonical_authorization != authorization_bytes(
        document
    ).decode("utf-8"):
        raise IdempotencyConflict("continuation authorization is unavailable")
    execution, lease, build = await _locked_retained(
        session, authorization_record.dataset_id, authorization_record.execution_id, authorization_record.build_id
    )
    now = await database_now(session)
    snapshot = await _snapshot_row(session, document, lock=False)
    _same_build(document, execution, build, snapshot)
    _eligible(execution, lease, build, snapshot, first=False)
    if (
        lease.continuation_id != authorization_record.continuation_id
        or lease.continuation_started_at is not None
        or lease.expires_at is None
        or lease.expires_at > now
        or authorization_record.launch_expires_at <= now
        or not hmac.compare_digest(bytes(authorization_record.nonce_sha256), lease_token_sha256(nonce))
        or lease.token_sha256 != authorization_record.nonce_sha256
    ):
        raise IdempotencyConflict("continuation launch is not current or was already consumed")
    await _require_base_and_capture(session, execution, build)
    request = await _request(session, authorization_record, build, nonce, lease_seconds)
    lease.continuation_started_at = lease.heartbeat_at = lease.updated_at = now
    lease.expires_at = now + dt.timedelta(seconds=lease_seconds)
    await session.flush()
    return request


async def _request(session, authorization_record, build, nonce, lease_seconds):
    from process.custom_import.build_source import SourceBuildRequest

    revision = await session.get(CustomImportDefinitionRevision, authorization_record.definition_revision_id)
    if (
        revision is None
        or revision.dataset_id != authorization_record.dataset_id
        or revision.schema_revision_id != authorization_record.schema_revision_id
    ):
        raise IdempotencyConflict("retained definition is unavailable")
    definition = CustomImportDefinition.from_json(revision.canonical_definition)
    if bytes(revision.definition_sha256) != bytes.fromhex(definition.digest):
        raise IdempotencyConflict("retained definition digest differs")
    return SourceBuildRequest(
        dataset_id=authorization_record.dataset_id,
        definition_revision_id=authorization_record.definition_revision_id,
        schema_revision_id=authorization_record.schema_revision_id,
        execution_id=authorization_record.execution_id,
        lease_token=nonce,
        fence=authorization_record.producer_fence,
        definition=definition,
        expected_base_generation_id=authorization_record.base_generation_id,
        expected_pointer_version=authorization_record.base_pointer_version,
        complete_scope=build.complete_scope,
        page_row_limit=build.page_row_limit,
        page_byte_limit=build.page_byte_limit,
        statement_timeout_ms=build.statement_timeout_ms,
        build_deadline_at=authorization_record.original_deadline_at,
        lease_seconds=lease_seconds,
        continuation=BuildContinuationContext(
            authorization_record.continuation_id, build.build_id, bytes(authorization_record.producer_token_sha256)
        ),
    )


async def inspect_build_continuation(session, *, build_id, **pins):
    """Return only exact retained identity and bounded progress, never bearer data."""

    build = await session.get(CustomImportBuildAttempt, build_id)
    execution = None if build is None else await session.get(CustomImportExecution, build.execution_id)
    if execution is None:
        raise IdempotencyConflict("retained build is unavailable")
    _same_request(execution, **pins)
    lease = await session.get(CustomImportLease, execution.execution_id)
    snapshot = (
        await session.scalars(
            select(CustomImportSnapshotFamily).where(
                CustomImportSnapshotFamily.execution_id == execution.execution_id,
                CustomImportSnapshotFamily.producing_fence == build.producing_fence,
            )
        )
    ).one_or_none()
    if lease is None or snapshot is None:
        raise IdempotencyConflict("retained authority is unavailable")
    authorization_record = (
        None
        if lease.continuation_id is None
        else await session.get(CustomImportBuildContinuation, lease.continuation_id)
    )
    is_sealed = (
        build.generation_id is not None
        and await session.get(CustomImportGenerationSeal, build.generation_id) is not None
    )
    return _status(execution, lease, build, snapshot, authorization_record, is_sealed=is_sealed)


def _timestamp(value):
    return None if value is None else value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _status(execution, lease, build, snapshot, authorization_record, *, is_sealed):
    retained_by_field = {
        key: getattr(build, key)
        for key in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "build_id",
            "capture_bundle_id",
            "base_generation_id",
            "base_pointer_version",
        )
    }
    retained_by_field.update(
        engine_execution_id=str(execution.execution_id),
        snapshot_family_id=snapshot.family_id,
        producer_fence=build.producing_fence,
        producer_token_sha256=bytes(build.producing_token_sha256).hex(),
        request_identity_sha256=bytes(build.request_identity_sha256).hex(),
        original_deadline_at=_timestamp(build.build_deadline_at),
    )
    current_by_field = {
        key: getattr(build, key)
        for key in ("phase", "selected_family_count", "completed_family_count", "generation_id")
    }
    current_by_field.update(
        continuation_id=lease.continuation_id,
        authorization_id=None if authorization_record is None else authorization_record.authorization_id,
        started_at=_timestamp(lease.continuation_started_at),
        lease_expires_at=_timestamp(lease.expires_at),
        engine_state=execution.state,
        sealed=is_sealed,
        processing_deadline_removed=authorization_record is not None,
    )
    return {
        "contract": "custom-import/build-continuation-status/v1",
        "retained": retained_by_field,
        "current": current_by_field,
    }
