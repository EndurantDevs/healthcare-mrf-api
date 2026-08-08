"""Validation and persistence for one materialized preclaim retirement."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from api.control_import_wave_attestation import (
    MATERIALIZED_PRECLAIM_ATTESTATION_VERSION,
)
from api.control_import_wave_supersession import _as_aware_utc
from db.models import PTGImportWaveQuarantine, PTGImportWaveSupersession
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    RECOVERY_BASIS,
    PTGWaveMaterializedPreclaimConflict,
    validate_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_materialized_preclaim_supersession_runtime import (
    attest_locked_materialized_preclaim_supersession,
)
from process.ptg_wave_state import canonical_json


QUARANTINE_REASON = "materialized_preclaim_failure"


async def is_materialized_preclaim_retired(
    executor: Any,
    wave_id: str,
    *,
    lock_row: bool = False,
) -> bool:
    """Return whether one durable wave has been permanently retired."""

    statement = select(PTGImportWaveQuarantine.predecessor_wave_id).where(
        PTGImportWaveQuarantine.predecessor_wave_id == wave_id
    )
    if lock_row:
        statement = statement.with_for_update()
    return (
        await executor.execute(statement)
    ).scalar_one_or_none() is not None


async def require_materialized_preclaim_replay_allowed(
    session: Any,
    wave_id: str,
) -> None:
    """Reject replay of a permanently retired predecessor identity."""

    if await is_materialized_preclaim_retired(
        session,
        wave_id,
        lock_row=True,
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "import wave admission identity is permanently retired"
        )


def validate_materialized_preclaim_supersession(
    attestation: dict[str, Any],
    *,
    wave_id: str,
) -> dict[str, Any] | None:
    """Validate the V5 proof and bind it to the new wave ID."""

    if attestation["schema_version"] != MATERIALIZED_PRECLAIM_ATTESTATION_VERSION:
        return None
    try:
        return validate_materialized_preclaim_supersession_proof(
            attestation["materialized_preclaim_supersession"],
            successor_wave_id=wave_id,
        )
    except PTGWaveMaterializedPreclaimConflict as exc:
        raise ValueError(str(exc)) from exc


async def persist_materialized_preclaim_supersession(
    session: Any,
    request: dict[str, Any],
    *,
    now: Any,
    redis: Any,
) -> None:
    """Re-attest, quarantine, and retire V10 in the V11 transaction."""

    proof = request.get("materialized_preclaim_supersession")
    if proof is None:
        return
    predecessor_wave_id = proof["predecessor"]["wave_id"]
    witness = await attest_locked_materialized_preclaim_supersession(
        session,
        predecessor_wave_id,
        request["wave_id"],
        proof,
        redis=redis,
    )
    session.add(
        PTGImportWaveQuarantine(
            predecessor_wave_id=predecessor_wave_id,
            reason=QUARANTINE_REASON,
            created_at=_as_aware_utc(now),
        )
    )
    await session.flush()
    session.add(
        PTGImportWaveSupersession(
            predecessor_wave_id=predecessor_wave_id,
            successor_wave_id=request["wave_id"],
            recovery_basis=RECOVERY_BASIS,
            recovery_evidence=witness,
            recovery_evidence_canonical=canonical_json({
                field_name: proof_field_value
                for field_name, proof_field_value in witness.items()
                if field_name != "proof_digest"
            }),
            recovery_evidence_sha256=witness["proof_digest"],
            created_at=_as_aware_utc(now),
        )
    )
    await session.flush()


__all__ = [
    "is_materialized_preclaim_retired",
    "persist_materialized_preclaim_supersession",
    "require_materialized_preclaim_replay_allowed",
    "validate_materialized_preclaim_supersession",
]
