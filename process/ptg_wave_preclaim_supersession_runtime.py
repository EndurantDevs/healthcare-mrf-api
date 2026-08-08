"""GET-only observation and locked revalidation for one recovery successor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from api.ptg_wave_kubernetes_client import get_wave_job
from db.models import (
    ImportRun,
    PTGImportWave,
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
    PTGImportWaveQuarantine,
    PTGImportWaveSupersession,
    db,
)
from process.ptg_wave_controller import PTGWaveBundle, restore_wave_manifest
from process.ptg_wave_failure_snapshots import _worker_start_event_ordinals
from process.ptg_wave_preclaim_supersession import (
    PTGWaveLogicalPreclaimSupersessionWitness,
    PTGWavePreclaimSupersessionConflict,
    attest_logical_preclaim_supersession,
    validate_logical_preclaim_supersession_proof,
)
from process.ptg_wave_redis import (
    attest_ptg_small_wave_unclaimed_failure_redis,
)


_QUARANTINE_REASON = "legacy_uncertain_slots_waiting_pre_receipt"


@dataclass(frozen=True)
class _PreclaimDatabaseSnapshot:
    wave: PTGImportWave
    intents: tuple[PTGImportWaveIntent, ...]
    runs: tuple[ImportRun, ...]
    claims: tuple[PTGImportWaveClaim, ...]
    outcomes: tuple[PTGImportWaveOutcome, ...]
    worker_start_event_ordinals: tuple[int, ...]


async def get_logical_preclaim_supersession_candidate(
    predecessor_wave_id: str,
    successor_wave_id: str,
    *,
    redis: Any,
) -> dict[str, Any]:
    """Return a GET-only proof candidate without changing engine state."""

    predecessor_wave_id = _wave_id(predecessor_wave_id, "predecessor wave ID")
    successor_wave_id = _wave_id(successor_wave_id, "successor wave ID")
    async with db.session() as session:
        existing = await _supersession_row(session, predecessor_wave_id)
        if existing is not None:
            if existing.successor_wave_id != successor_wave_id:
                raise PTGWavePreclaimSupersessionConflict(
                    "predecessor is already bound to another successor"
                )
            return validate_logical_preclaim_supersession_proof(
                existing.recovery_evidence,
                predecessor_wave_id=predecessor_wave_id,
                successor_wave_id=successor_wave_id,
            )
        snapshot = await _load_preclaim_database_snapshot(
            session,
            predecessor_wave_id,
            lock_rows=False,
        )
    witness = await _observe_external_preclaim_state(
        snapshot,
        successor_wave_id,
        redis=redis,
    )
    return witness.as_mapping()


async def attest_locked_logical_preclaim_supersession(
    session: Any,
    predecessor_wave_id: str,
    successor_wave_id: str,
    expected_proof: Any,
    *,
    redis: Any,
) -> PTGWaveLogicalPreclaimSupersessionWitness:
    """Reobserve and match a signed proof while authoritative rows are locked."""

    predecessor_wave_id = _wave_id(predecessor_wave_id, "predecessor wave ID")
    successor_wave_id = _wave_id(successor_wave_id, "successor wave ID")
    expected = validate_logical_preclaim_supersession_proof(
        expected_proof,
        predecessor_wave_id=predecessor_wave_id,
        successor_wave_id=successor_wave_id,
    )
    if await _supersession_row(session, predecessor_wave_id, lock_row=True) is not None:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor already has an immutable supersession"
        )
    snapshot = await _load_preclaim_database_snapshot(
        session,
        predecessor_wave_id,
        lock_rows=True,
    )
    witness = await _observe_external_preclaim_state(
        snapshot,
        successor_wave_id,
        redis=redis,
    )
    if witness.as_mapping() != expected:
        raise PTGWavePreclaimSupersessionConflict(
            "signed logical pre-claim proof differs from the current exact observation"
        )
    return witness


async def _load_preclaim_database_snapshot(
    session: Any,
    predecessor_wave_id: str,
    *,
    lock_rows: bool,
) -> _PreclaimDatabaseSnapshot:
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
    if wave is None or quarantine is None or quarantine.reason != _QUARANTINE_REASON:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor is not the quarantined legacy pre-receipt wave"
        )
    intents_statement = (
        select(PTGImportWaveIntent)
        .where(PTGImportWaveIntent.wave_id == predecessor_wave_id)
        .order_by(PTGImportWaveIntent.ordinal)
    )
    if lock_rows:
        intents_statement = intents_statement.with_for_update()
    intents = tuple((await session.execute(intents_statement)).scalars().all())
    run_ids = [intent.run_id for intent in intents]
    runs_statement = select(ImportRun).where(ImportRun.run_id.in_(run_ids)).order_by(
        ImportRun.run_id
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
        runs_statement = runs_statement.with_for_update()
        claims_statement = claims_statement.with_for_update()
        outcomes_statement = outcomes_statement.with_for_update()
    runs = tuple((await session.execute(runs_statement)).scalars().all())
    claims = tuple((await session.execute(claims_statement)).scalars().all())
    outcomes = tuple((await session.execute(outcomes_statement)).scalars().all())
    worker_events = tuple(await _worker_start_event_ordinals(session, intents))
    return _PreclaimDatabaseSnapshot(
        wave=wave,
        intents=intents,
        runs=runs,
        claims=claims,
        outcomes=outcomes,
        worker_start_event_ordinals=worker_events,
    )


async def _observe_external_preclaim_state(
    snapshot: _PreclaimDatabaseSnapshot,
    successor_wave_id: str,
    *,
    redis: Any,
) -> PTGWaveLogicalPreclaimSupersessionWitness:
    if redis is None:
        raise PTGWavePreclaimSupersessionConflict(
            "logical pre-claim proof requires the exact-wave Redis observer"
        )
    actual_job = get_wave_job(snapshot.wave.wave_digest)
    if actual_job is None:
        raise PTGWavePreclaimSupersessionConflict(
            "predecessor Kubernetes Job is unavailable"
        )
    manifest = restore_wave_manifest(
        PTGWaveBundle(wave=snapshot.wave, intents=snapshot.intents)
    )
    redis_attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        manifest,
    )
    return attest_logical_preclaim_supersession(
        snapshot.wave,
        snapshot.intents,
        snapshot.runs,
        snapshot.claims,
        snapshot.outcomes,
        snapshot.worker_start_event_ordinals,
        actual_job,
        redis_attestation.as_mapping(),
        successor_wave_id,
    )


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


def _wave_id(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 64
    ):
        raise PTGWavePreclaimSupersessionConflict(
            f"{name} must be a non-empty bounded string"
        )
    return value


__all__ = [
    "attest_locked_logical_preclaim_supersession",
    "get_logical_preclaim_supersession_candidate",
]
