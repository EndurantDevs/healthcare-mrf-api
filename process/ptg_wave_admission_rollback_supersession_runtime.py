"""GET-only observation and locked revalidation for absent admissions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import os
from typing import Any

from arq.constants import health_check_key_suffix
from sqlalchemy import or_, select, text

from api.ptg_wave_kubernetes import PTGWaveContractError, _job_name
from api.ptg_wave_kubernetes_client import (
    KubernetesApiError,
    wave_absence_observation,
)
from db.models import (
    PTGImportWave,
    PTGImportWaveAdmissionRollback,
    db,
)
from process._ptg_wave_redis_encoding import (
    wave_ready_key,
    wave_release_key,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    PTGWaveAdmissionRollbackConflict,
    build_admission_rollback_supersession_proof,
    validate_admission_rollback_predecessor,
    validate_admission_rollback_supersession_proof,
)


_TAGGED_RUN_PREDICATE = """
    (
        import_run.params::jsonb->>'_wave_id' = :wave_id
        OR import_run.metrics::jsonb->>'wave_id' = :wave_id
        OR import_run.params::jsonb->>'_wave_digest' = :wave_digest
        OR import_run.metrics::jsonb->>'wave_digest' = :wave_digest
    )
"""
_DATABASE_ABSENCE_QUERY = """
    SELECT
      (SELECT count(*) FROM {wave_table}
        WHERE wave_id = :wave_id) AS wave_id_count,
      (SELECT count(*) FROM {wave_table}
        WHERE idempotency_key = :idempotency_key) AS idempotency_key_count,
      (SELECT count(*) FROM {wave_table}
        WHERE request_digest = :request_digest) AS request_digest_count,
      (SELECT count(*) FROM {wave_table}
        WHERE wave_digest = :wave_digest) AS wave_digest_count,
      (SELECT count(*) FROM {intent_table}
        WHERE wave_id = :wave_id) AS intent_count,
      (SELECT count(*) FROM {claim_table}
        WHERE wave_id = :wave_id) AS claim_count,
      (SELECT count(*) FROM {outcome_table}
        WHERE wave_id = :wave_id) AS outcome_count,
      (SELECT count(*) FROM {run_table} AS import_run
        WHERE {tagged_run_predicate}) AS wave_tagged_import_run_count,
      (SELECT count(*)
         FROM {event_table} AS attempt_event
         JOIN {run_table} AS import_run
           ON import_run.run_id = attempt_event.outer_run_id
        WHERE attempt_event.event_kind = 'worker_start_admitted'
          AND {tagged_run_predicate})
        AS wave_tagged_worker_start_event_count,
      (SELECT count(*) FROM {supersession_table}
        WHERE predecessor_wave_id = :wave_id)
        AS supersession_predecessor_count,
      (SELECT count(*) FROM {supersession_table}
        WHERE successor_wave_id = :wave_id)
        AS supersession_successor_count,
      (SELECT count(*) FROM {retirement_table}
        WHERE predecessor_wave_id = :wave_id
           OR predecessor_idempotency_key = :idempotency_key
           OR predecessor_request_digest = :request_digest
           OR predecessor_wave_digest = :wave_digest)
        AS retirement_count
"""


async def get_admission_rollback_supersession_candidate(
    predecessor: Any,
    successor_wave_id: str,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Return a GET-only successor-bound proof without changing state."""

    descriptor = validate_admission_rollback_predecessor(predecessor)
    async with db.session() as session:
        existing = await _retirement_row(
            session,
            descriptor["wave_id"],
        )
        if existing is not None:
            return _validated_existing_retirement(
                existing,
                descriptor,
                successor_wave_id,
            )
        database = await _database_absence_observation(
            session,
            descriptor,
        )
    kubernetes, redis_proof = await _external_absence_observation(
        descriptor,
        redis=redis,
    )
    return build_admission_rollback_supersession_proof(
        descriptor,
        successor_wave_id,
        database=database,
        kubernetes=kubernetes,
        redis=redis_proof,
    )


async def attest_locked_admission_rollback_supersession(
    session: Any,
    predecessor: Any,
    successor_wave_id: str,
    expected_proof: Any,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Reobserve one expected absence proof under the admission lock."""

    descriptor = validate_admission_rollback_predecessor(predecessor)
    expected = validate_admission_rollback_supersession_proof(
        expected_proof,
        predecessor=descriptor,
        successor_wave_id=successor_wave_id,
    )
    if await _retirement_row(
        session,
        descriptor["wave_id"],
        lock_row=True,
    ) is not None:
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor already has an immutable admission rollback"
        )
    database = await _database_absence_observation(session, descriptor)
    kubernetes, redis_proof = await _external_absence_observation(
        descriptor,
        redis=redis,
    )
    observed = build_admission_rollback_supersession_proof(
        descriptor,
        successor_wave_id,
        database=database,
        kubernetes=kubernetes,
        redis=redis_proof,
    )
    if observed != expected:
        raise PTGWaveAdmissionRollbackConflict(
            "signed admission rollback proof differs from current observation"
        )
    return observed


async def find_admission_retirement_collision(
    session: Any,
    request: Mapping[str, Any],
    *,
    lock_row: bool = True,
) -> PTGImportWaveAdmissionRollback | None:
    """Return a tombstone matching any immutable request identity."""

    statement = select(PTGImportWaveAdmissionRollback).where(or_(
        PTGImportWaveAdmissionRollback.predecessor_wave_id
        == request["wave_id"],
        PTGImportWaveAdmissionRollback.predecessor_idempotency_key
        == request["idempotency_key"],
        PTGImportWaveAdmissionRollback.predecessor_request_digest
        == request["request_digest"],
        PTGImportWaveAdmissionRollback.predecessor_wave_digest
        == request["wave_digest"],
    )).limit(2)
    if lock_row:
        statement = statement.with_for_update()
    rows = list((await session.execute(statement)).scalars())
    if len(rows) > 1:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback identities match different tombstones"
        )
    return rows[0] if rows else None


async def _database_absence_observation(
    session: Any,
    descriptor: Mapping[str, Any],
) -> dict[str, int]:
    """Count every database artifact addressable by the descriptor."""

    statement = text(_DATABASE_ABSENCE_QUERY.format(
        **_database_tables_by_name(),
        tagged_run_predicate=_TAGGED_RUN_PREDICATE,
    ))
    observation_row = (
        await session.execute(
            statement,
            {
                "wave_id": descriptor["wave_id"],
                "idempotency_key": descriptor["idempotency_key"],
                "request_digest": descriptor["request_digest"],
                "wave_digest": descriptor["wave_digest"],
            },
        )
    ).one()
    observation_values = (
        observation_row._mapping
        if hasattr(observation_row, "_mapping")
        else observation_row
    )
    try:
        return {
            name: int(observation_values[name])
            for name in DATABASE_FIELDS
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback database observation is invalid"
        ) from exc


def _database_tables_by_name() -> dict[str, str]:
    """Resolve the configured schema into quoted database table names."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise PTGWaveAdmissionRollbackConflict(
            "database schema configuration is inconsistent"
        )
    schema = _quote_ident(
        runtime_schema
        or legacy_schema
        or PTGImportWave.__table__.schema
        or "mrf"
    )
    return {
        "wave_table": f"{schema}.{_quote_ident('ptg_import_wave')}",
        "intent_table": f"{schema}.{_quote_ident('ptg_import_wave_intent')}",
        "claim_table": f"{schema}.{_quote_ident('ptg_import_wave_claim')}",
        "outcome_table": f"{schema}.{_quote_ident('ptg_import_wave_outcome')}",
        "run_table": f"{schema}.{_quote_ident('import_run')}",
        "event_table": f"{schema}.{_quote_ident('ptg_source_attempt_event')}",
        "supersession_table": (
            f"{schema}.{_quote_ident('ptg_import_wave_supersession')}"
        ),
        "retirement_table": (
            f"{schema}.{_quote_ident('ptg_import_wave_admission_rollback')}"
        ),
    }


async def _external_absence_observation(
    descriptor: Mapping[str, Any],
    *,
    redis: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if redis is None:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback requires the exact-wave Redis observer"
        )
    try:
        kubernetes_observation = wave_absence_observation(
            descriptor["wave_digest"]
        )
    except (KubernetesApiError, PTGWaveContractError) as exc:
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback Kubernetes observation failed"
        ) from exc
    kubernetes_map = {
        "job_name": _job_name(descriptor["wave_digest"]),
        "job_present": not bool(kubernetes_observation.get("job_absent")),
        "pod_count": kubernetes_observation.get("pod_count"),
    }
    redis_proof = await _redis_absence_observation(redis, descriptor)
    return kubernetes_map, redis_proof


async def _redis_absence_observation(
    redis: Any,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    wave_digest = descriptor["wave_digest"]
    queue_name = descriptor["release_queue"]
    async with redis.pipeline(transaction=True) as pipe:
        pipe.zcard(queue_name)
        pipe.hlen(wave_ready_key(wave_digest))
        pipe.get(wave_release_key(wave_digest))
        pipe.get(queue_name + health_check_key_suffix)
        raw_values = await pipe.execute(raise_on_error=False)
    if (
        not isinstance(raw_values, Sequence)
        or isinstance(raw_values, (str, bytes, bytearray))
        or len(raw_values) != 4
        or type(raw_values[0]) is not int
        or type(raw_values[1]) is not int
    ):
        raise PTGWaveAdmissionRollbackConflict(
            "admission rollback Redis observation is invalid"
        )
    return {
        "queue_name": queue_name,
        "queued_entry_count": raw_values[0],
        "ready_slot_count": raw_values[1],
        "release_present": raw_values[2] is not None,
        "health_check_present": raw_values[3] is not None,
    }


async def _retirement_row(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_row: bool = False,
) -> PTGImportWaveAdmissionRollback | None:
    statement = select(PTGImportWaveAdmissionRollback).where(
        PTGImportWaveAdmissionRollback.predecessor_wave_id
        == predecessor_wave_id
    )
    if lock_row:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


def _validated_existing_retirement(
    retirement: PTGImportWaveAdmissionRollback,
    descriptor: Mapping[str, Any],
    successor_wave_id: str,
) -> dict[str, Any]:
    stored_descriptor_map = {
        "wave_id": retirement.predecessor_wave_id,
        "idempotency_key": retirement.predecessor_idempotency_key,
        "request_digest": retirement.predecessor_request_digest,
        "wave_digest": retirement.predecessor_wave_digest,
        "release_queue": retirement.predecessor_release_queue,
        "intent_count": retirement.predecessor_intent_count,
    }
    if stored_descriptor_map != dict(descriptor):
        raise PTGWaveAdmissionRollbackConflict(
            "predecessor descriptor conflicts with its immutable rollback"
        )
    return validate_admission_rollback_supersession_proof(
        retirement.recovery_evidence,
        predecessor=descriptor,
        successor_wave_id=successor_wave_id,
    )


__all__ = [
    "attest_locked_admission_rollback_supersession",
    "find_admission_retirement_collision",
    "get_admission_rollback_supersession_candidate",
]
