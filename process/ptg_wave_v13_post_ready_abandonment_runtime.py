"""Locked, read-only observation for V13 post-ready failure quarantine."""

from __future__ import annotations

from typing import Any

from sqlalchemy import bindparam, or_, select, text

from api.ptg_wave_kubernetes_client import get_wave_job, list_wave_pods
from db.migration_ptg2_legacy_v3_metadata_reconcile import EVENT_TABLE
from db.models import (
    ImportRun,
    PTGImportWave,
    PTGImportWaveAdmissionRollback,
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
    PTGImportWaveQuarantine,
    PTGImportWaveSupersession,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_wave_controller import PTGWaveBundle, restore_wave_manifest
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_receipt_contract import (
    PTGWaveReceiptContractError,
    admission_receipt_mapping,
)
from process.ptg_wave_redis import attest_ptg_small_wave_unclaimed_failure_redis
from process.ptg_wave_v13_post_ready_abandonment import (
    PTGWaveV13PostReadyObservation,
    attest_v13_post_ready_abandonment,
    validate_v13_abandonment_request,
)


async def attest_locked_v13_abandonment(
    session: Any,
    wave_id: str,
    request: object,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Observe only locked DB rows plus GET-only Kubernetes and Redis state."""

    if redis is None:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 abandonment requires the Redis observer"
        )
    _require_wave_id(wave_id)
    try:
        return await _attest_locked_observation(
            session,
            wave_id,
            request,
            redis,
        )
    except PTGWaveMaterializedPreclaimConflict:
        raise
    except PTGWaveReceiptContractError as exc:
        raise PTGWaveMaterializedPreclaimConflict(str(exc)) from exc
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 post-ready abandonment observation failed"
        ) from exc


async def _attest_locked_observation(
    session: Any,
    wave_id: str,
    request: object,
    redis: Any,
) -> dict[str, Any]:
    """Read the locked predecessor evidence and build its closed V13 proof."""

    await _lock_v13_evidence_tables(session)
    wave = await _locked_wave(session, wave_id)
    intents = await _locked_intents(session, wave_id)
    runs = await _locked_runs(session, wave_id)
    claims = await _locked_rows(session, PTGImportWaveClaim, wave_id)
    outcomes = await _locked_rows(session, PTGImportWaveOutcome, wave_id)
    worker_events = await _locked_worker_start_event_ordinals(session, intents)
    logical, rollback = await _locked_recovery_rows(session, wave_id)
    admission = admission_receipt_mapping(wave, intents)
    validated_request = validate_v13_abandonment_request(
        request,
        wave=wave,
        admission=admission,
    )
    actual_job = get_wave_job(wave.wave_digest)
    if actual_job is None:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor Kubernetes Job is unavailable"
        )
    manifest = restore_wave_manifest(PTGWaveBundle(wave=wave, intents=intents))
    redis_attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        manifest,
    )
    return attest_v13_post_ready_abandonment(
        PTGWaveV13PostReadyObservation(
            predecessor_wave=wave,
            intents=intents,
            runs=runs,
            claims=claims,
            outcomes=outcomes,
            worker_start_event_ordinals=worker_events,
            logical_supersession=logical,
            admission_rollback=rollback,
            actual_job=actual_job,
            actual_pods=list_wave_pods(wave.wave_digest),
            redis_unclaimed_attestation=redis_attestation.as_mapping(),
        ),
        cutover_id=validated_request["cutover_id"],
        admission=admission,
    )


async def _lock_v13_evidence_tables(session: Any) -> None:
    """Prevent concurrent lifecycle writes while the immutable proof is read."""

    schema = _quote_ident(resolve_ptg2_schema())
    table_names = (
        PTGImportWave.__tablename__,
        PTGImportWaveQuarantine.__tablename__,
        PTGImportWaveIntent.__tablename__,
        PTGImportWaveClaim.__tablename__,
        PTGImportWaveOutcome.__tablename__,
        ImportRun.__tablename__,
        PTGImportWaveSupersession.__tablename__,
        PTGImportWaveAdmissionRollback.__tablename__,
        EVENT_TABLE,
    )
    tables = ", ".join(
        f"{schema}.{_quote_ident(table_name)}" for table_name in table_names
    )
    await session.execute(
        text(f"LOCK TABLE {tables} IN SHARE ROW EXCLUSIVE MODE")
    )


async def _locked_wave(session: Any, wave_id: str) -> PTGImportWave:
    wave = (
        await session.execute(
            select(PTGImportWave)
            .where(PTGImportWave.wave_id == wave_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    quarantine = (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.predecessor_wave_id == wave_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if wave is None or quarantine is not None:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor is missing or already quarantined"
        )
    return wave


async def _locked_intents(
    session: Any,
    wave_id: str,
) -> tuple[PTGImportWaveIntent, ...]:
    return tuple(
        (
            await session.execute(
                select(PTGImportWaveIntent)
                .where(PTGImportWaveIntent.wave_id == wave_id)
                .order_by(PTGImportWaveIntent.ordinal)
                .with_for_update()
            )
        ).scalars().all()
    )


async def _locked_runs(session: Any, wave_id: str) -> tuple[ImportRun, ...]:
    return tuple(
        (
            await session.execute(
                select(ImportRun)
                .join(
                    PTGImportWaveIntent,
                    ImportRun.run_id == PTGImportWaveIntent.run_id,
                )
                .where(PTGImportWaveIntent.wave_id == wave_id)
                .order_by(PTGImportWaveIntent.ordinal)
                .with_for_update()
            )
        ).scalars().all()
    )


async def _locked_rows(session: Any, model: Any, wave_id: str) -> tuple[Any, ...]:
    return tuple(
        (
            await session.execute(
                select(model)
                .where(model.wave_id == wave_id)
                .order_by(model.ordinal)
                .with_for_update()
            )
        ).scalars().all()
    )


async def _locked_worker_start_event_ordinals(
    session: Any,
    intents: tuple[PTGImportWaveIntent, ...],
) -> tuple[int, ...]:
    ordinal_by_run_id = {intent.run_id: int(intent.ordinal) for intent in intents}
    event_table = f'{_quote_ident(resolve_ptg2_schema())}.{_quote_ident(EVENT_TABLE)}'
    event_rows = (
        await session.execute(
            text(
                f"""
                SELECT outer_run_id
                  FROM {event_table}
                 WHERE event_kind = 'worker_start_admitted'
                   AND outer_run_id IN :run_ids
                 ORDER BY outer_run_id
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": list(ordinal_by_run_id)},
        )
    ).all()
    try:
        return tuple(
            sorted(
                {
                    ordinal_by_run_id[
                        event_row._mapping["outer_run_id"]
                        if hasattr(event_row, "_mapping")
                        else event_row[0]
                    ]
                    for event_row in event_rows
                }
            )
        )
    except (KeyError, TypeError) as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 worker-event observation is invalid"
        ) from exc


async def _locked_recovery_rows(
    session: Any,
    wave_id: str,
) -> tuple[
    PTGImportWaveSupersession | None,
    PTGImportWaveAdmissionRollback | None,
]:
    logical = (
        await session.execute(
            select(PTGImportWaveSupersession)
            .where(
                or_(
                    PTGImportWaveSupersession.predecessor_wave_id == wave_id,
                    PTGImportWaveSupersession.successor_wave_id == wave_id,
                )
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    rollback = (
        await session.execute(
            select(PTGImportWaveAdmissionRollback)
            .where(
                or_(
                    PTGImportWaveAdmissionRollback.predecessor_wave_id == wave_id,
                    PTGImportWaveAdmissionRollback.successor_wave_id == wave_id,
                )
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    return logical, rollback


def _require_wave_id(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 64
    ):
        raise PTGWaveMaterializedPreclaimConflict("fresh V13 wave ID is invalid")
    return value


__all__ = ["attest_locked_v13_abandonment"]
