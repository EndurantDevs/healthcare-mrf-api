"""Validation and persistence for one absent-admission retirement."""

from __future__ import annotations

import datetime as dt
from typing import Any

from api.control_import_wave_attestation import ROLLBACK_ATTESTATION_VERSION
from api.control_import_wave_supersession import _as_aware_utc
from db.models import PTGImportWaveAdmissionRollback
from process.ptg_wave_admission_rollback_supersession import (
    PTGWaveAdmissionRollbackConflict,
    RECOVERY_BASIS,
    validate_admission_rollback_supersession_proof,
)
from process.ptg_wave_admission_rollback_supersession_runtime import (
    attest_locked_admission_rollback_supersession,
)
from process.ptg_wave_state import canonical_json


def validate_admission_rollback_supersession(
    attestation: dict[str, Any],
    *,
    wave_id: str,
) -> dict[str, Any] | None:
    """Validate and bind the V4 rollback proof to its successor."""

    if attestation["schema_version"] != ROLLBACK_ATTESTATION_VERSION:
        return None
    try:
        return validate_admission_rollback_supersession_proof(
            attestation["admission_rollback_supersession"],
            successor_wave_id=wave_id,
        )
    except PTGWaveAdmissionRollbackConflict as exc:
        raise ValueError(str(exc)) from exc


async def persist_admission_rollback_supersession(
    session: Any,
    request: dict[str, Any],
    *,
    now: dt.datetime,
    redis: Any = None,
) -> None:
    """Re-attest and persist a V4 predecessor tombstone before capacity."""

    rollback_proof = request.get("admission_rollback_supersession")
    if rollback_proof is None:
        return
    predecessor = rollback_proof["predecessor"]
    witness = await attest_locked_admission_rollback_supersession(
        session,
        predecessor,
        request["wave_id"],
        rollback_proof,
        redis=redis,
    )
    session.add(PTGImportWaveAdmissionRollback(
        predecessor_wave_id=predecessor["wave_id"],
        predecessor_idempotency_key=predecessor["idempotency_key"],
        predecessor_request_digest=predecessor["request_digest"],
        predecessor_wave_digest=predecessor["wave_digest"],
        predecessor_release_queue=predecessor["release_queue"],
        predecessor_intent_count=predecessor["intent_count"],
        successor_wave_id=request["wave_id"],
        recovery_basis=RECOVERY_BASIS,
        recovery_evidence=witness,
        recovery_evidence_canonical=canonical_json({
            name: proof_field_value
            for name, proof_field_value in witness.items()
            if name != "proof_digest"
        }),
        recovery_evidence_sha256=witness["proof_digest"],
        created_at=_as_aware_utc(now),
    ))
    await session.flush()


__all__ = [
    "persist_admission_rollback_supersession",
    "validate_admission_rollback_supersession",
]
