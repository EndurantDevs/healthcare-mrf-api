"""Audited cutover from one pristine exact wave to ordinary PTG admission."""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select

from api.control_import_wave_supersession import _as_aware_utc
from db.models import PTGImportWaveQuarantine, db
from process.ptg_parts.ptg_wave_admission_fence import (
    acquire_ptg_admission_lock,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
    validate_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_materialized_preclaim_supersession_runtime import (
    attest_locked_materialized_preclaim_abandonment,
)
from process.ptg_wave_state import canonical_json


QUARANTINE_REASON = "materialized_preclaim_failure"


async def abandon_materialized_preclaim_wave(
    wave_id: object,
    cutover_id: object,
    *,
    redis: Any,
) -> tuple[dict[str, Any], bool]:
    """Atomically quarantine one all-unclaimed wave without a successor.

    ``cutover_id`` is caller-owned idempotency identity.  It is bound into the
    existing materialized-preclaim proof format, but no successor wave or
    supersession row is created.
    """

    normalized_wave_id = _identity(wave_id, "wave ID")
    normalized_cutover_id = _identity(cutover_id, "cutover ID")
    if normalized_cutover_id == normalized_wave_id:
        raise ValueError("cutover ID must differ from the wave ID")

    async with db.transaction() as session:
        await acquire_ptg_admission_lock(session)
        existing = await _locked_quarantine(session, normalized_wave_id)
        if existing is not None:
            return _existing_response(
                existing,
                wave_id=normalized_wave_id,
                cutover_id=normalized_cutover_id,
            ), False
        cutover_owner = await _locked_cutover_owner(
            session,
            normalized_cutover_id,
        )
        if cutover_owner is not None:
            raise PTGWaveMaterializedPreclaimConflict(
                "cutover ID is already bound to another wave"
            )
        return await _persist_abandonment(
            session,
            normalized_wave_id,
            normalized_cutover_id,
            redis=redis,
        )


async def _locked_quarantine(session: Any, wave_id: str) -> Any | None:
    """Lock and return an existing quarantine for one wave identity."""

    return (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.predecessor_wave_id == wave_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _locked_cutover_owner(session: Any, cutover_id: str) -> Any | None:
    """Lock and return the wave already bound to one cutover identity."""

    return (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.cutover_id == cutover_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _persist_abandonment(
    session: Any,
    wave_id: str,
    cutover_id: str,
    *,
    redis: Any,
) -> tuple[dict[str, Any], bool]:
    """Persist the exact proof and return its first-write response."""

    witness = await attest_locked_materialized_preclaim_abandonment(
        session,
        wave_id,
        cutover_id,
        redis=redis,
    )
    canonical = canonical_json({
        field_name: field_value
        for field_name, field_value in witness.items()
        if field_name != "proof_digest"
    })
    session.add(
        PTGImportWaveQuarantine(
            predecessor_wave_id=wave_id,
            reason=QUARANTINE_REASON,
            cutover_id=cutover_id,
            recovery_basis=QUARANTINE_REASON,
            recovery_evidence=witness,
            recovery_evidence_canonical=canonical,
            recovery_evidence_sha256=witness["proof_digest"],
            created_at=_as_aware_utc(dt.datetime.now(dt.UTC)),
        )
    )
    await session.flush()
    return _response(
        wave_id=wave_id,
        cutover_id=cutover_id,
        proof=witness,
        proof_digest=witness["proof_digest"],
        created=True,
    ), True


def _existing_response(
    quarantine: Any,
    *,
    wave_id: str,
    cutover_id: str,
) -> dict[str, Any]:
    if (
        getattr(quarantine, "reason", None) != QUARANTINE_REASON
        or getattr(quarantine, "recovery_basis", None) != QUARANTINE_REASON
        or getattr(quarantine, "cutover_id", None) != cutover_id
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    proof = validate_materialized_preclaim_supersession_proof(
        getattr(quarantine, "recovery_evidence", None),
        predecessor_wave_id=wave_id,
        successor_wave_id=cutover_id,
    )
    proof_digest = getattr(quarantine, "recovery_evidence_sha256", None)
    if proof_digest != proof["proof_digest"]:
        raise PTGWaveMaterializedPreclaimConflict(
            "stored abandonment proof digest is invalid"
        )
    return _response(
        wave_id=wave_id,
        cutover_id=cutover_id,
        proof=proof,
        proof_digest=proof_digest,
        created=False,
    )


def _response(
    *,
    wave_id: str,
    cutover_id: str,
    proof: dict[str, Any],
    proof_digest: str,
    created: bool,
) -> dict[str, Any]:
    database = proof["database"]
    redis = proof["redis"]
    run_count = database["pristine_run_count"]
    return {
        "wave_id": wave_id,
        "cutover_id": cutover_id,
        "state": "abandoned",
        "quarantine_reason": QUARANTINE_REASON,
        "quarantined_run_count": run_count,
        "unclaimed_run_count": run_count,
        "queued_run_count": run_count,
        "claim_count": database["claim_count"],
        "outcome_count": database["outcome_count"],
        "worker_start_event_count": database[
            "worker_start_event_count"
        ],
        "redis_release_present": redis["release_present"],
        "proof_digest": proof_digest,
        "created": created,
    }


def _identity(value: object, name: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 64
    ):
        raise ValueError(f"{name} is invalid")
    return value


__all__ = ["abandon_materialized_preclaim_wave"]
