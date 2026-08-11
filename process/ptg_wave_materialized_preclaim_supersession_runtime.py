"""GET-only observation and locked retirement of a materialized failed wave."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from api.ptg_wave_kubernetes_client import get_wave_job
from db.models import (
    ImportRun,
    PTGImportWave,
    PTGImportWaveAdmissionRollback,
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
    PTGImportWaveQuarantine,
    PTGImportWaveSupersession,
    db,
)
from process.ptg_wave_controller import PTGWaveBundle, restore_wave_manifest
from process.ptg_wave_failure_snapshots import _worker_start_event_ordinals
from process.ptg_wave_materialized_preclaim_supersession import (
    PTGWaveMaterializedPreclaimObservation,
    attest_materialized_preclaim_supersession,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
    RECOVERY_BASIS,
    validate_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_receipt_contract import (
    PTGWaveReceiptContractError,
    admission_receipt_mapping,
    validate_abandonment_request,
)
from process.ptg_wave_redis import (
    attest_ptg_small_wave_unclaimed_failure_redis,
)
from process.ptg_wave_v12_pristine_abandonment import (
    attest_v12_pristine_materialized_abandonment,
)


_QUARANTINE_REASON = "materialized_preclaim_failure"


@dataclass(frozen=True)
class _MaterializedDatabaseSnapshot:
    wave: PTGImportWave
    intents: tuple[PTGImportWaveIntent, ...]
    runs: tuple[ImportRun, ...]
    claims: tuple[PTGImportWaveClaim, ...]
    outcomes: tuple[PTGImportWaveOutcome, ...]
    worker_start_event_ordinals: tuple[int, ...]
    logical_supersession: PTGImportWaveSupersession | None
    admission_rollback: PTGImportWaveAdmissionRollback | None


async def get_materialized_preclaim_supersession_candidate(
    predecessor_wave_id: str,
    successor_wave_id: str,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Return a successor-bound proof without changing any durable state."""

    predecessor_wave_id = _wave_id(predecessor_wave_id, "predecessor wave ID")
    successor_wave_id = _wave_id(successor_wave_id, "successor wave ID")
    async with db.session() as session:
        existing = await _supersession_row(session, predecessor_wave_id)
        if existing is not None:
            return _existing_proof(
                existing,
                predecessor_wave_id=predecessor_wave_id,
                successor_wave_id=successor_wave_id,
            )
        snapshot = await _load_snapshot(
            session, predecessor_wave_id, lock_rows=False
        )
    return await _observe(snapshot, successor_wave_id, redis=redis)


async def attest_locked_materialized_preclaim_supersession(
    session: Any,
    predecessor_wave_id: str,
    successor_wave_id: str,
    expected_proof: Any,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Reobserve the signed proof while all predecessor rows are locked."""

    predecessor_wave_id = _wave_id(predecessor_wave_id, "predecessor wave ID")
    successor_wave_id = _wave_id(successor_wave_id, "successor wave ID")
    expected = validate_materialized_preclaim_supersession_proof(
        expected_proof,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
    )
    if await _supersession_row(
        session, predecessor_wave_id, lock_row=True
    ) is not None:
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor already has an immutable supersession"
        )
    snapshot = await _load_snapshot(
        session, predecessor_wave_id, lock_rows=True
    )
    observed = await _observe(snapshot, successor_wave_id, redis=redis)
    if observed != expected:
        raise PTGWaveMaterializedPreclaimConflict(
            "signed materialized preclaim proof differs from current state"
        )
    return observed


async def attest_locked_materialized_preclaim_abandonment(
    session: Any,
    predecessor_wave_id: str,
    cutover_id: str,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Observe one pristine materialized wave for a non-wave cutover.

    The existing proof format is deliberately reused.  Its
    ``successor_wave_id`` slot binds the immutable proof to ``cutover_id``;
    this function does not create or require a successor wave row.
    """

    predecessor_wave_id = _wave_id(predecessor_wave_id, "predecessor wave ID")
    cutover_id = _wave_id(cutover_id, "cutover ID")
    if cutover_id == predecessor_wave_id:
        raise PTGWaveMaterializedPreclaimConflict(
            "cutover ID must differ from the predecessor wave ID"
        )
    if await _supersession_row(
        session, predecessor_wave_id, lock_row=True
    ) is not None:
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor already has an immutable supersession"
        )
    snapshot = await _load_snapshot(
        session, predecessor_wave_id, lock_rows=True
    )
    if any(
        getattr(run, "node_id", None) is not None
        for run in snapshot.runs
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor runs must be unassigned"
        )
    return await _observe(snapshot, cutover_id, redis=redis)


async def attest_locked_v12_abandonment(
    session: Any,
    wave_id: str,
    request: object,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Reobserve a fresh-v6 pristine wave without legacy recovery lineage."""

    wave_id = _wave_id(wave_id, "wave ID")
    if await _supersession_row(session, wave_id, lock_row=True) is not None:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 wave already has an immutable supersession"
        )
    snapshot = await _load_snapshot(session, wave_id, lock_rows=True)
    try:
        admission = admission_receipt_mapping(
            snapshot.wave,
            snapshot.intents,
        )
        validated_request = validate_abandonment_request(
            request,
            wave=snapshot.wave,
            admission=admission,
        )
    except PTGWaveReceiptContractError as exc:
        raise PTGWaveMaterializedPreclaimConflict(str(exc)) from exc
    if any(getattr(run, "node_id", None) is not None for run in snapshot.runs):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 materialized runs must be unassigned"
        )
    return await _observe_v12(
        snapshot,
        cutover_id=validated_request["cutover_id"],
        admission=admission,
        redis=redis,
    )


async def _load_snapshot(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_rows: bool,
) -> _MaterializedDatabaseSnapshot:
    """Read the predecessor boundary in one fixed lock order."""

    wave = await _load_predecessor_wave(
        session,
        predecessor_wave_id,
        lock_rows=lock_rows,
    )
    intents, runs, claims, outcomes, worker_events = await _load_work_rows(
        session,
        predecessor_wave_id,
        lock_rows=lock_rows,
    )
    logical, rollback = await _load_prior_recovery_rows(
        session,
        predecessor_wave_id,
        lock_rows=lock_rows,
    )
    return _MaterializedDatabaseSnapshot(
        wave=wave,
        intents=intents,
        runs=runs,
        claims=claims,
        outcomes=outcomes,
        worker_start_event_ordinals=worker_events,
        logical_supersession=logical,
        admission_rollback=rollback,
    )


async def _load_predecessor_wave(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_rows: bool,
) -> PTGImportWave:
    wave_statement = select(PTGImportWave).where(
        PTGImportWave.wave_id == predecessor_wave_id
    )
    quarantine_statement = select(PTGImportWaveQuarantine).where(
        PTGImportWaveQuarantine.predecessor_wave_id == predecessor_wave_id
    )
    if lock_rows:
        wave_statement = wave_statement.with_for_update()
        quarantine_statement = quarantine_statement.with_for_update()
    wave = (await session.execute(wave_statement)).scalar_one_or_none()
    quarantine = (
        await session.execute(quarantine_statement)
    ).scalar_one_or_none()
    if wave is None or quarantine is not None:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor is missing or already quarantined"
        )
    return wave


async def _load_work_rows(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_rows: bool,
) -> tuple[
    tuple[PTGImportWaveIntent, ...],
    tuple[ImportRun, ...],
    tuple[PTGImportWaveClaim, ...],
    tuple[PTGImportWaveOutcome, ...],
    tuple[int, ...],
]:
    intents_statement = (
        select(PTGImportWaveIntent)
        .where(PTGImportWaveIntent.wave_id == predecessor_wave_id)
        .order_by(PTGImportWaveIntent.ordinal)
    )
    claims_statement = (
        select(PTGImportWaveClaim)
        .where(PTGImportWaveClaim.wave_id == predecessor_wave_id)
        .order_by(PTGImportWaveClaim.ordinal)
    )
    outcomes_statement = (
        select(PTGImportWaveOutcome)
        .where(PTGImportWaveOutcome.wave_id == predecessor_wave_id)
        .order_by(PTGImportWaveOutcome.ordinal)
    )
    if lock_rows:
        intents_statement = intents_statement.with_for_update()
        claims_statement = claims_statement.with_for_update()
        outcomes_statement = outcomes_statement.with_for_update()
    intents = tuple((await session.execute(intents_statement)).scalars().all())
    run_ids = [intent.run_id for intent in intents]
    runs_statement = select(ImportRun).where(
        ImportRun.run_id.in_(run_ids)
    ).order_by(ImportRun.run_id)
    if lock_rows:
        runs_statement = runs_statement.with_for_update()
    runs = tuple((await session.execute(runs_statement)).scalars().all())
    claims = tuple((await session.execute(claims_statement)).scalars().all())
    outcomes = tuple((await session.execute(outcomes_statement)).scalars().all())
    try:
        worker_events = tuple(
            await _worker_start_event_ordinals(session, intents)
        )
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized predecessor worker events are invalid"
        ) from exc
    return intents, runs, claims, outcomes, worker_events


async def _load_prior_recovery_rows(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_rows: bool,
) -> tuple[
    PTGImportWaveSupersession | None,
    PTGImportWaveAdmissionRollback | None,
]:
    logical_statement = select(PTGImportWaveSupersession).where(
        PTGImportWaveSupersession.successor_wave_id == predecessor_wave_id
    )
    rollback_statement = select(PTGImportWaveAdmissionRollback).where(
        PTGImportWaveAdmissionRollback.successor_wave_id
        == predecessor_wave_id
    )
    if lock_rows:
        logical_statement = logical_statement.with_for_update()
        rollback_statement = rollback_statement.with_for_update()
    logical = (await session.execute(logical_statement)).scalar_one_or_none()
    rollback = (await session.execute(rollback_statement)).scalar_one_or_none()
    return logical, rollback


async def _observe(
    snapshot: _MaterializedDatabaseSnapshot,
    successor_wave_id: str,
    *,
    redis: Any,
) -> dict[str, Any]:
    if redis is None:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim proof requires the Redis observer"
        )
    try:
        actual_job = get_wave_job(snapshot.wave.wave_digest)
        if actual_job is None:
            raise PTGWaveMaterializedPreclaimConflict(
                "materialized predecessor Kubernetes Job is unavailable"
            )
        manifest = restore_wave_manifest(
            PTGWaveBundle(wave=snapshot.wave, intents=snapshot.intents)
        )
        redis_attestation = (
            await attest_ptg_small_wave_unclaimed_failure_redis(
                redis,
                manifest,
            )
        )
        return attest_materialized_preclaim_supersession(
            PTGWaveMaterializedPreclaimObservation(
                predecessor_wave=snapshot.wave,
                intents=snapshot.intents,
                runs=snapshot.runs,
                claims=snapshot.claims,
                outcomes=snapshot.outcomes,
                worker_start_event_ordinals=(
                    snapshot.worker_start_event_ordinals
                ),
                logical_supersession=snapshot.logical_supersession,
                admission_rollback=snapshot.admission_rollback,
                actual_job=actual_job,
                redis_unclaimed_attestation=redis_attestation.as_mapping(),
            ),
            successor_wave_id,
        )
    except PTGWaveMaterializedPreclaimConflict:
        raise
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "materialized preclaim observation failed"
        ) from exc


async def _observe_v12(
    snapshot: _MaterializedDatabaseSnapshot,
    *,
    cutover_id: str,
    admission: dict[str, Any],
    redis: Any,
) -> dict[str, Any]:
    """Observe the external V12 boundary and build its distinct proof."""

    if redis is None:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment requires the Redis observer"
        )
    try:
        actual_job = get_wave_job(snapshot.wave.wave_digest)
        if actual_job is None:
            raise PTGWaveMaterializedPreclaimConflict(
                "fresh V12 predecessor Kubernetes Job is unavailable"
            )
        manifest = restore_wave_manifest(
            PTGWaveBundle(wave=snapshot.wave, intents=snapshot.intents)
        )
        redis_attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
            redis,
            manifest,
        )
        return attest_v12_pristine_materialized_abandonment(
            PTGWaveMaterializedPreclaimObservation(
                predecessor_wave=snapshot.wave,
                intents=snapshot.intents,
                runs=snapshot.runs,
                claims=snapshot.claims,
                outcomes=snapshot.outcomes,
                worker_start_event_ordinals=(
                    snapshot.worker_start_event_ordinals
                ),
                logical_supersession=snapshot.logical_supersession,
                admission_rollback=snapshot.admission_rollback,
                actual_job=actual_job,
                redis_unclaimed_attestation=redis_attestation.as_mapping(),
            ),
            cutover_id=cutover_id,
            admission=admission,
        )
    except PTGWaveMaterializedPreclaimConflict:
        raise
    except Exception as exc:
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 abandonment observation failed"
        ) from exc


async def _supersession_row(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_row: bool = False,
) -> PTGImportWaveSupersession | None:
    statement = select(PTGImportWaveSupersession).where(
        PTGImportWaveSupersession.predecessor_wave_id == predecessor_wave_id
    )
    if lock_row:
        statement = statement.with_for_update()
    return (await session.execute(statement)).scalar_one_or_none()


def _existing_proof(
    existing: PTGImportWaveSupersession,
    *,
    predecessor_wave_id: str,
    successor_wave_id: str,
) -> dict[str, Any]:
    if (
        existing.recovery_basis != RECOVERY_BASIS
        or existing.successor_wave_id != successor_wave_id
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "predecessor is already bound to another recovery"
        )
    return validate_materialized_preclaim_supersession_proof(
        existing.recovery_evidence,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
    )


def _wave_id(value: Any, name: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 64
    ):
        raise PTGWaveMaterializedPreclaimConflict(f"{name} is invalid")
    return value


__all__ = [
    "attest_locked_materialized_preclaim_abandonment",
    "attest_locked_v12_abandonment",
    "attest_locked_materialized_preclaim_supersession",
    "get_materialized_preclaim_supersession_candidate",
]
