"""Signed, durable admission for exact PTGSmall waves.

This module accepts no publication callback.  It only records an immutable,
authenticated complete-cohort request and its exact ARQ bytes.  A separate
controller will later reconcile materialization and publication without ever
re-serializing the admitted jobs.
"""

from __future__ import annotations

import datetime as dt
import hmac
from typing import Any

from arq.jobs import serialize_job as arq_serialize_job
from sqlalchemy import func, select

from api.control_frozen_rate_files import validated_control_import_payload
from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    ROLLBACK_ATTESTATION_VERSION,
    SUPERSESSION_ATTESTATION_VERSION,
    _canonical,
    _identifier,
    _sha256,
    _validate_partition,
    _validate_snapshot,
    _verify_attestation,
    sign_cohort_attestation,
)
from api.control_imports import (
    _assert_ptg_rebuild_request_params,
    _import_param_views,
    _normalize_triggered_by,
)
from api import control_import_wave_direct as direct_wave
from api.control_import_wave_response import wave_response as _wave_response
from api.control_import_wave_recovery import (
    persist_admission_recoveries,
    validate_admission_recovery_proofs,
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


QUEUE = "arq:PTGSmall"
WORKER_CLASS = "process.PTGSmall"
RESOURCE_CLASS = "small"
WORKER_LIMIT = 12
MAX_INTENTS = 4096
MAX_INTENT_CANONICAL_BYTES = direct_wave.MAX_INTENT_CANONICAL_BYTES
MAX_ATTESTATION_CANONICAL_BYTES = direct_wave.MAX_ATTESTATION_CANONICAL_BYTES
PROTOCOL_IDENTITY = "healthporta.ptg-small.exact-wave.v1"
SERIALIZER_IDENTITY = "arq-0.28.process-msgpack.v1"
class ImportWaveConflict(ValueError):
    """A replay attempted to change a previously admitted immutable wave."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _job_id(wave_id: str, request_digest: str, ordinal: int, run_id: str) -> str:
    identity = _sha256(f"{wave_id}\0{request_digest}\0{ordinal}\0{run_id}".encode())
    return f"ptg_start_{identity}"


def _run_key(wave_id: str, request_digest: str, ordinal: int) -> str:
    return "ptg-wave:" + _sha256(f"{wave_id}\0{request_digest}\0{ordinal}".encode())


def _canonical_job_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        if not all(isinstance(key, str) for key in payload):
            raise ValueError("ARQ job payload keys must be strings")
        return {key: _canonical_job_payload(payload[key]) for key in sorted(payload)}
    if isinstance(payload, list):
        return [_canonical_job_payload(item) for item in payload]
    if isinstance(payload, (str, int, bool)) or payload is None:
        return payload
    if isinstance(payload, float):
        if not payload == payload or payload in (float("inf"), float("-inf")):
            raise ValueError("ARQ job payload contains a non-finite float")
        return payload
    raise ValueError("ARQ job payload contains an unsupported value")


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
            "run_id": run_id,
            "importer": "ptg",
            "params": raw_params,
            "idempotency_key": run_key,
            "triggered_by": "api",
            "source_file_import_id": source_id,
            "import_id": source_id,
        }
    )
    actual_source_id = source_file_import_id_from_payload(
        control_payload,
        required=True,
    )
    if actual_source_id != source_id:
        raise ValueError("intent source_file_import_id does not match its PTG parameters")
    return source_id, control_payload


def _prepare_intent(
    intent: dict[str, Any], *, wave_id: str, request_digest: str,
    wave_digest: str, release_queue: str, ordinal: int, enqueue_time_ms: int, now: dt.datetime,
) -> dict[str, Any]:
    """Prepare one immutable run, enqueue payload, and serialized ARQ job."""

    run_id = intent["run_id"]
    job_id = _job_id(wave_id, request_digest, ordinal, run_id)
    run_key = _run_key(wave_id, request_digest, ordinal)
    source_id, control_payload = _validated_intent_payload(
        intent,
        run_id=run_id,
        run_key=run_key,
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


def _validate_signed_intents(raw_intents: object, *, wave_id: str | None = None) -> list[dict[str, Any]]:
    if not isinstance(raw_intents, list) or not 1 <= len(raw_intents) <= MAX_INTENTS:
        raise ValueError(f"cohort_attestation intents must contain between 1 and {MAX_INTENTS} items")
    intents: list[dict[str, Any]] = []
    run_ids: set[str] = set()
    source_ids: set[str] = set()
    for ordinal, raw in enumerate(raw_intents):
        direct_wave.require_bounded_direct_intent(raw)
        expected_intent_fields = {
            "ordinal", "run_id", "source_file_import_id", "content_version", "params",
        }
        if not isinstance(raw, dict) or set(raw) != expected_intent_fields:
            raise ValueError("each signed intent fields are not exact")
        if raw["ordinal"] != ordinal:
            raise ValueError("signed intent ordinals must be contiguous from zero")
        run_id = _identifier(raw["run_id"], "run_id", 64)
        source_id = _identifier(raw["source_file_import_id"], "source_file_import_id", 64)
        if run_id in run_ids or source_id in source_ids:
            raise ValueError("signed intent run_id and source_file_import_id values must be unique")
        run_ids.add(run_id)
        source_ids.add(source_id)
        if not isinstance(raw["params"], dict):
            raise ValueError("signed intent params must be an object")
        content_version = _identifier(raw["content_version"], "content_version", 128)
        normalized_params = direct_wave.normalized_wave_params(raw["params"])
        direct_wave.require_matching_direct_coordinate(
            normalized_params,
            content_version,
            source_file_import_id=source_id,
            wave_id=wave_id or "",
        )
        intents.append({
            "run_id": run_id, "source_file_import_id": source_id,
            "content_version": content_version,
            "params": normalized_params,
        })
    return intents


def validate_import_wave_payload(
    request_body: object, *, attestation_key: str | bytes | None = None,
) -> dict[str, Any]:
    """Verify the closed orchestrator attestation and derive all identities."""

    if not isinstance(request_body, dict) or set(request_body) != {"cohort_attestation"}:
        raise ValueError("import wave payload must contain only cohort_attestation")
    direct_wave.require_bounded_wave_request(request_body)
    attestation = _verify_attestation(
        request_body["cohort_attestation"],
        attestation_key=attestation_key,
    )
    wave_id = _identifier(attestation["wave_id"], "wave_id", 64)
    idempotency_key = _identifier(attestation["idempotency_key"], "idempotency_key", 160)
    snapshot = _validate_snapshot(
        attestation["snapshot"],
        schema_version=attestation["schema_version"],
    )
    partition = _validate_partition(attestation["partition"])
    intents = _validate_signed_intents(
        attestation["intents"],
        wave_id=wave_id,
    )
    supersession, admission_rollback = validate_admission_recovery_proofs(
        attestation,
        wave_id=wave_id,
    )
    if partition["imported_coordinate_count"] != len(intents):
        raise ValueError("partition imported_coordinate_count must equal signed intent count")
    imported_coordinate_digest = _sha256(
        "\0".join(
            f"{intent['source_file_import_id']}\0{intent['content_version']}"
            for intent in intents
        ).encode("utf-8")
    )
    if not hmac.compare_digest(
        partition["imported_coordinate_digest"], imported_coordinate_digest
    ):
        raise ValueError("partition imported_coordinate_digest does not match signed intents")
    unsigned_attestation_map = {
        key: intent_field_value
        for key, intent_field_value in attestation.items()
        if key != "signature"
    }
    request_digest = _sha256(_canonical(unsigned_attestation_map))
    wave_digest = _sha256((PROTOCOL_IDENTITY + "\0" + request_digest).encode())
    validated_request_map = {
        "wave_id": wave_id, "idempotency_key": idempotency_key,
        "attestation": attestation, "snapshot": snapshot, "partition": partition,
        "supersession": supersession,
        "intents": intents, "request_digest": request_digest,
        "attestation_digest": _sha256(_canonical(attestation)),
        "signature_digest": _sha256(attestation["signature"].encode()),
        "wave_digest": wave_digest, "release_queue": f"{QUEUE}:wave:{wave_digest}",
    }
    if attestation["schema_version"] == ROLLBACK_ATTESTATION_VERSION:
        validated_request_map["admission_rollback_supersession"] = (
            admission_rollback
        )
    return validated_request_map


class _SessionExecutor:
    def __init__(self, session: Any) -> None:
        self.session = session

    async def execute(self, statement: Any, parameters: dict[str, Any] | None = None):
        """Execute one statement through the active admission transaction."""

        return await self.session.execute(statement, parameters or {})

    async def scalar(self, statement: Any, *args: Any, **parameters: Any):
        """Return one scalar through the active admission transaction."""

        values = dict(args[0]) if args else {}
        values.update(parameters)
        return await self.session.scalar(statement, values)

    async def all(self, statement: Any, **parameters: Any):
        """Return all rows through the active admission transaction."""

        return (await self.session.execute(statement, parameters)).all()

    async def status(self, statement: Any, **parameters: Any):
        """Return the affected-row count for a transactional statement."""

        return (await self.session.execute(statement, parameters)).rowcount


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
) -> tuple[dict[str, Any], bool]:
    """Atomically admit one authenticated complete wave or return its exact replay."""

    request = validate_import_wave_payload(admission_request)
    now = _now()
    enqueue_time_ms = int(now.replace(tzinfo=dt.UTC).timestamp() * 1000)
    prepared_intents, jobs_digest, manifest_digest = _prepare_wave_intents(
        request,
        now=now,
        enqueue_time_ms=enqueue_time_ms,
    )
    async with db.transaction() as session:
        executor = _SessionExecutor(session)
        await require_source_attempt_capabilities(executor, require_attempt_authority=False)
        for source_id in sorted(
            intent_entry["source_id"] for intent_entry in prepared_intents
        ):
            await guard_source_attempt(executor, source_file_import_id=source_id)
        await acquire_ptg_admission_lock(executor)
        existing_rows = list((await session.execute(
            select(PTGImportWave).where((PTGImportWave.wave_id == request["wave_id"]) |
                                         (PTGImportWave.idempotency_key == request["idempotency_key"])).limit(2).with_for_update()
        )).scalars())
        if len(existing_rows) > 1:
            raise ImportWaveConflict("wave_id and idempotency_key identify different immutable waves")
        existing = existing_rows[0] if existing_rows else None
        if existing is not None:
            if existing.request_digest != request["request_digest"]:
                raise ImportWaveConflict("wave_id or idempotency_key conflicts with immutable request digest")
            return _wave_response(existing), False
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


async def get_import_wave(wave_id: str) -> dict[str, Any] | None:
    """Return one durable wave projection without mutating its state."""

    result = await db.execute(select(PTGImportWave).where(
        PTGImportWave.wave_id == _identifier(wave_id, "wave_id", 64)))
    wave = result.scalar_one_or_none()
    return _wave_response(wave) if wave is not None else None


__all__ = [
    "ATTESTATION_VERSION", "ROLLBACK_ATTESTATION_VERSION", "SUPERSESSION_ATTESTATION_VERSION", "ImportWaveConflict", "admit_import_wave", "get_import_wave",
    "sign_cohort_attestation", "validate_import_wave_payload",
]
