"""Signed, durable admission for exact PTGSmall waves.

This module accepts no publication callback.  It only records an immutable,
authenticated complete-cohort request and its exact ARQ bytes.  A separate
controller will later reconcile materialization and publication without ever
re-serializing the admitted jobs.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from arq.jobs import serialize_job as arq_serialize_job
from sqlalchemy import select

from api import control_import_wave_direct as direct_wave
from api.control_frozen_rate_files import validated_control_import_payload
from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    MATERIALIZED_PRECLAIM_ATTESTATION_VERSION,
    RECEIPT_ATTESTATION_VERSION,
    ROLLBACK_ATTESTATION_VERSION,
    SUPERSESSION_ATTESTATION_VERSION,
    _canonical,
    _sha256,
    sign_cohort_attestation,
)
from api.control_import_wave_constants import (
    MAX_ATTESTATION_CANONICAL_BYTES,
    MAX_INTENT_CANONICAL_BYTES,
    MAX_INTENTS,
    PROTOCOL_IDENTITY,
    QUEUE,
    RESOURCE_CLASS,
    SERIALIZER_IDENTITY,
    WORKER_CLASS,
    WORKER_LIMIT,
)
from api.control_import_wave_materialized_preclaim import (
    require_materialized_preclaim_replay_allowed,
)
from api.control_import_wave_payload import (
    _canonical_job_payload,
    _job_id,
    _project_import_wave_payload,
    _run_key,
    _validate_signed_intents,
    validate_import_wave_payload,
)
from api.control_import_wave_session import SessionExecutor as _SessionExecutor
from api.control_import_wave_response import (
    get_import_wave,
    wave_response as _wave_response,
)
from api.control_import_wave_recovery import (
    persist_admission_recoveries,
)
from api.control_imports import (
    _assert_ptg_rebuild_request_params,
    _import_param_views,
    _normalize_triggered_by,
)
from db.models import (
    ImportRun,
    PTGImportWave,
    PTGImportWaveIntent,
    db,
)
from process.ptg_parts.frozen_rate_binding_store import insert_or_compare_frozen_binding
from process.ptg_parts.ptg_source_attempt_actions import record_source_attempt_event
from process.ptg_parts.ptg_source_attempt_guard import (
    guard_source_attempt,
    require_source_attempt_capabilities,
    source_file_import_id_from_payload,
)
from process.ptg_parts.ptg_wave_admission_fence import (
    acquire_ptg_admission_lock,
    require_wave_admission_capacity,
)
from process.serialization import serialize_job
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptPublicEpoch,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)


class ImportWaveConflict(ValueError):
    """A replay attempted to change a previously admitted immutable wave."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _validated_intent_payload(
    intent: dict[str, Any],
    *,
    run_id: str,
    run_key: str,
) -> tuple[str, dict[str, Any]]:
    source_id = intent["source_file_import_id"]
    raw_params = direct_wave.normalized_wave_params(intent["params"])
    _assert_ptg_rebuild_request_params("ptg", raw_params)
    control_payload = validated_control_import_payload(
        {
            "run_id": run_id, "importer": "ptg", "params": raw_params,
            "idempotency_key": run_key, "triggered_by": "api",
            "source_file_import_id": source_id, "import_id": source_id,
        }
    )
    actual_source_id = source_file_import_id_from_payload(
        control_payload,
        required=True,
    )
    if actual_source_id != source_id:
        raise ValueError(
            "intent source_file_import_id does not match its PTG parameters"
        )
    return source_id, control_payload


def _prepare_intent(
    intent: dict[str, Any], *, wave_id: str, request_digest: str,
    wave_digest: str, release_queue: str, ordinal: int,
    enqueue_time_ms: int, now: dt.datetime,
) -> dict[str, Any]:
    """Prepare one immutable run, enqueue payload, and serialized ARQ job."""
    run_id = intent["run_id"]
    job_id = _job_id(wave_id, request_digest, ordinal, run_id)
    run_key = _run_key(wave_id, request_digest, ordinal)
    source_id, control_payload = _validated_intent_payload(
        intent, run_id=run_id, run_key=run_key,
    )
    views = _import_param_views("ptg", control_payload["params"], run_id=run_id)
    persisted_parameter_map = {
        **views.persisted_by_name, "content_version": intent["content_version"],
        "resource_class": RESOURCE_CLASS, "_expected_queue": release_queue,
        "_expected_worker_class": WORKER_CLASS, "_wave_id": wave_id,
        "_wave_digest": wave_digest, "_wave_job_id": job_id,
    }
    enqueue_parameter_map = {
        **views.enqueue_by_name, "content_version": intent["content_version"],
        "resource_class": RESOURCE_CLASS, "_expected_queue": release_queue,
        "_expected_worker_class": WORKER_CLASS, "_wave_id": wave_id,
        "_wave_digest": wave_digest, "_wave_job_id": job_id,
    }
    job_payload = _canonical_job_payload({
        "run_id": run_id, "source_file_import_id": source_id,
        "import_id": source_id, "params": enqueue_parameter_map,
    })
    serialized_job = arq_serialize_job(
        "ptg_control_start", (job_payload,), {}, None, enqueue_time_ms,
        serializer=serialize_job,
    )
    run_field_map = {
        "run_id": run_id, "engine": "healthcare-mrf-api", "node_id": None,
        "importer": "ptg", "family": "pricing", "status": "queued",
        "phase_detail": "wave admitted; controller materialization pending",
        "params": persisted_parameter_map, "idempotency_key": run_key,
        "triggered_by": _normalize_triggered_by(control_payload.get("triggered_by")),
        "schedule_id": None, "subscription_id": None,
        "source_file_import_id": source_id, "created_at": now, "heartbeat_at": now,
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0,
                     "message": "wave admitted; controller materialization pending"},
        "metrics": {"wave_id": wave_id, "queue": release_queue,
                    "base_queue": QUEUE, "worker_class": WORKER_CLASS,
                    "resource_class": RESOURCE_CLASS, "worker_limit": WORKER_LIMIT,
                    "job_id": job_id, "ordinal": ordinal,
                    "wave_digest": wave_digest},
        "error": None, "snapshot_id": None, "import_id": source_id,
        "retry_of_run_id": None,
    }
    return {
        "ordinal": ordinal, "run_id": run_id, "source_id": source_id,
        "content_version": intent["content_version"], "job_id": job_id,
        "run_key": run_key, "persisted_params": persisted_parameter_map,
        "job_payload": job_payload, "serialized_job": serialized_job,
        "serialized_job_digest": _sha256(serialized_job), "run_values": run_field_map,
    }


def _manifest_digests(prepared: list[dict[str, Any]], *, wave_digest: str, enqueue_time_ms: int) -> tuple[str, str]:
    jobs = [
        {"ordinal": item["ordinal"], "job_id": item["job_id"],
         "score_ms": enqueue_time_ms,
         "serialized_job_digest": item["serialized_job_digest"]}
        for item in prepared
    ]
    jobs_digest = _sha256(_canonical({"schema_version": 1, "protocol_identity": PROTOCOL_IDENTITY,
                                     "serializer_identity": SERIALIZER_IDENTITY, "jobs": jobs}))
    manifest_digest = _sha256(_canonical({"schema_version": 1, "wave_id": wave_digest,
                                          "queue_name": f"{QUEUE}:wave:{wave_digest}",
                                          "enqueue_time_ms": enqueue_time_ms, "job_count": len(jobs),
                                          "jobs_digest": jobs_digest, "protocol_identity": PROTOCOL_IDENTITY,
                                          "serializer_identity": SERIALIZER_IDENTITY}))
    return jobs_digest, manifest_digest


def _prepare_wave_intents(
    request: dict[str, Any],
    *,
    now: dt.datetime,
    enqueue_time_ms: int,
) -> tuple[list[dict[str, Any]], str, str]:
    prepared_intents = [
        _prepare_intent(
            intent,
            wave_id=request["wave_id"],
            request_digest=request["request_digest"],
            wave_digest=request["wave_digest"],
            release_queue=request["release_queue"],
            ordinal=ordinal,
            enqueue_time_ms=enqueue_time_ms,
            now=now,
        )
        for ordinal, intent in enumerate(request["intents"])
    ]
    jobs_digest, manifest_digest = _manifest_digests(
        prepared_intents,
        wave_digest=request["wave_digest"],
        enqueue_time_ms=enqueue_time_ms,
    )
    return prepared_intents, jobs_digest, manifest_digest


def _new_wave_record(
    request: dict[str, Any],
    prepared_intents: list[dict[str, Any]],
    *,
    jobs_digest: str,
    manifest_digest: str,
    enqueue_time_ms: int,
    now: dt.datetime,
    receipt_public_epoch: PTGWaveReceiptPublicEpoch | None = None,
) -> PTGImportWave:
    partition = request["partition"]
    return PTGImportWave(
        wave_id=request["wave_id"], idempotency_key=request["idempotency_key"],
        request_digest=request["request_digest"], cohort_attestation=request["attestation"],
        cohort_attestation_digest=request["attestation_digest"],
        cohort_signature_digest=request["signature_digest"],
        physical_coordinate_count=partition["physical_coordinate_count"],
        physical_coordinate_digest=partition["physical_coordinate_digest"],
        partition_digest=partition["partition_digest"],
        imported_coordinate_count=partition["imported_coordinate_count"],
        imported_coordinate_digest=partition["imported_coordinate_digest"],
        reused_coordinate_count=partition["reused_coordinate_count"],
        reused_coordinate_digest=partition["reused_coordinate_digest"],
        intent_count=len(prepared_intents), jobs_digest=jobs_digest,
        manifest_digest=manifest_digest, wave_digest=request["wave_digest"],
        receipt_key_id=request["receipt_key_id"],
        receipt_public_modulus_hex=(
            receipt_public_epoch.rsa_modulus
            if receipt_public_epoch is not None
            else request["receipt_public_modulus_hex"]
        ),
        receipt_public_exponent=(
            receipt_public_epoch.rsa_exponent
            if receipt_public_epoch is not None
            else request["receipt_public_exponent"]
        ),
        queue=QUEUE, release_queue=request["release_queue"], worker_class=WORKER_CLASS,
        resource_class=RESOURCE_CLASS, worker_limit=WORKER_LIMIT,
        protocol_identity=PROTOCOL_IDENTITY, serializer_identity=SERIALIZER_IDENTITY,
        enqueue_time_ms=enqueue_time_ms, state="admitted", created_at=now,
    )


async def _persist_wave_intents(
    session: Any,
    executor: _SessionExecutor,
    wave: PTGImportWave,
    prepared_intents: list[dict[str, Any]],
) -> None:
    session.add(wave)
    for intent_entry in prepared_intents:
        await insert_or_compare_frozen_binding(
            executor,
            intent_entry["persisted_params"],
        )
        session.add(ImportRun(**intent_entry["run_values"]))
        session.add(PTGImportWaveIntent(
            wave_id=wave.wave_id, ordinal=intent_entry["ordinal"],
            run_id=intent_entry["run_id"], source_file_import_id=intent_entry["source_id"],
            content_version=intent_entry["content_version"],
            run_idempotency_key=intent_entry["run_key"], job_id=intent_entry["job_id"],
            params=intent_entry["persisted_params"], job_payload=intent_entry["job_payload"],
            serialized_job=intent_entry["serialized_job"],
            serialized_job_digest=intent_entry["serialized_job_digest"],
        ))


async def admit_import_wave(
    admission_request: object,
    *,
    redis: Any = None,
    receipt_keyring: PTGWaveReceiptKeyring | None = None,
) -> tuple[dict[str, Any], bool]:
    """Atomically admit one authenticated complete wave or return its exact replay."""

    request, authentication_error = _project_admission_request(
        admission_request
    )
    async with db.transaction() as session:
        executor = _SessionExecutor(session)
        await acquire_ptg_admission_lock(executor)
        replay_response = await _locked_existing_wave_response(session, request)
        if replay_response is not None:
            return replay_response, False
        if authentication_error is not None:
            raise authentication_error
        now = _now()
        enqueue_time_ms = int(
            now.replace(tzinfo=dt.UTC).timestamp() * 1000
        )
        prepared_intents, jobs_digest, manifest_digest, receipt_public_epoch = (
            await _prepare_guarded_admission_intents(
                executor,
                request,
                now=now,
                enqueue_time_ms=enqueue_time_ms,
                receipt_keyring=receipt_keyring,
            )
        )
        await persist_admission_recoveries(
            session,
            request,
            now=now,
            redis=redis,
        )
        await require_wave_admission_capacity(executor)
        wave = _new_wave_record(
            request,
            prepared_intents,
            jobs_digest=jobs_digest,
            manifest_digest=manifest_digest,
            enqueue_time_ms=enqueue_time_ms,
            now=now,
            receipt_public_epoch=receipt_public_epoch,
        )
        await _persist_wave_intents(session, executor, wave, prepared_intents)
        await session.flush()
        for intent_entry in prepared_intents:
            await record_source_attempt_event(
                executor,
                source_file_import_id=intent_entry["source_id"],
                event_kind="start_admitted",
                outer_run=intent_entry["run_values"],
            )
        return _wave_response(wave), True


def _project_admission_request(
    admission_request: object,
) -> tuple[dict[str, Any], ValueError | None]:
    """Authenticate a new request while retaining V6 replay identity."""

    raw_attestation = (
        admission_request.get("cohort_attestation")
        if isinstance(admission_request, dict)
        else None
    )
    is_v6_request = bool(
        isinstance(raw_attestation, dict)
        and raw_attestation.get("schema_version")
        == RECEIPT_ATTESTATION_VERSION
    )
    try:
        return validate_import_wave_payload(admission_request), None
    except ValueError as exc:
        if not is_v6_request:
            raise
        return (
            _project_import_wave_payload(
                admission_request,
                authenticate=False,
            ),
            exc,
        )


async def _locked_existing_wave_response(
    session: Any,
    request: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return an exact locked replay or reject immutable identity drift."""

    existing_rows = list(
        (
            await session.execute(
                select(PTGImportWave)
                .where(
                    (PTGImportWave.wave_id == request["wave_id"])
                    | (PTGImportWave.idempotency_key == request["idempotency_key"])
                )
                .limit(2)
                .with_for_update()
            )
        ).scalars()
    )
    if len(existing_rows) > 1:
        raise ImportWaveConflict(
            "wave_id and idempotency_key identify different immutable waves"
        )
    existing = existing_rows[0] if existing_rows else None
    if existing is None:
        return None
    if (
        existing.request_digest != request["request_digest"]
        or getattr(existing, "receipt_key_id", None) != request["receipt_key_id"]
        or getattr(existing, "receipt_public_modulus_hex", None)
        != request.get("receipt_public_modulus_hex")
        or getattr(existing, "receipt_public_exponent", None)
        != request.get("receipt_public_exponent")
        or existing.cohort_attestation_digest != request["attestation_digest"]
        or existing.cohort_signature_digest != request["signature_digest"]
        or _canonical(existing.cohort_attestation)
        != _canonical(request["attestation"])
    ):
        raise ImportWaveConflict(
            "wave_id or idempotency_key conflicts with immutable request digest"
        )
    await require_materialized_preclaim_replay_allowed(session, existing.wave_id)
    return _wave_response(existing)


async def _prepare_guarded_admission_intents(
    executor: _SessionExecutor,
    request: Mapping[str, Any],
    *,
    now: dt.datetime,
    enqueue_time_ms: int,
    receipt_keyring: PTGWaveReceiptKeyring | None,
) -> tuple[list[dict[str, Any]], str, str, Any]:
    """Prepare source-local rows, fence attempts, and pin active receipt trust."""

    prepared_intents, jobs_digest, manifest_digest = _prepare_wave_intents(
        request,
        now=now,
        enqueue_time_ms=enqueue_time_ms,
    )
    await require_source_attempt_capabilities(
        executor,
        require_attempt_authority=False,
    )
    for source_id in sorted(
        intent_entry["source_id"] for intent_entry in prepared_intents
    ):
        await guard_source_attempt(executor, source_file_import_id=source_id)
    receipt_public_epoch = None
    if request["receipt_key_id"] is not None:
        keyring = require_process_receipt_keyring(receipt_keyring)
        receipt_public_epoch = keyring.require_active_public_material(
            key_id=request["receipt_key_id"],
            modulus=request["receipt_public_modulus_hex"],
            exponent=request["receipt_public_exponent"],
        )
    return prepared_intents, jobs_digest, manifest_digest, receipt_public_epoch


__all__ = [
    "ATTESTATION_VERSION", "MATERIALIZED_PRECLAIM_ATTESTATION_VERSION", "RECEIPT_ATTESTATION_VERSION", "ROLLBACK_ATTESTATION_VERSION", "SUPERSESSION_ATTESTATION_VERSION", "ImportWaveConflict", "admit_import_wave", "get_import_wave",
    "sign_cohort_attestation", "validate_import_wave_payload",
]
