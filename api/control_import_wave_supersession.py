"""Preclaim supersession validation and persistence during wave admission."""

from __future__ import annotations

import datetime as dt
from typing import Any

from api.control_import_wave_attestation import SUPERSESSION_ATTESTATION_VERSION
from db.models import PTGImportWaveSupersession
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimSupersessionConflict,
    validate_logical_preclaim_supersession_proof,
)
from process.ptg_wave_preclaim_supersession_runtime import (
    attest_locked_logical_preclaim_supersession,
)
from process.ptg_wave_state import canonical_json


def validate_admission_supersession(
    attestation: dict[str, Any],
    *,
    wave_id: str,
) -> dict[str, Any] | None:
    """Validate and bind a v3 supersession proof to its successor wave."""

    if attestation["schema_version"] != SUPERSESSION_ATTESTATION_VERSION:
        return None
    try:
        return validate_logical_preclaim_supersession_proof(
            attestation["supersession"],
            successor_wave_id=wave_id,
        )
    except PTGWavePreclaimSupersessionConflict as exc:
        raise ValueError(str(exc)) from exc


async def persist_admission_supersession(
    session: Any,
    request: dict[str, Any],
    *,
    now: dt.datetime,
    redis: Any = None,
) -> None:
    """Re-attest and persist a successor-bound proof before capacity admission."""

    supersession_proof = request.get("supersession")
    if supersession_proof is None:
        return
    predecessor_wave_id = supersession_proof["predecessor"]["wave_id"]
    witness = await attest_locked_logical_preclaim_supersession(
        session,
        predecessor_wave_id,
        request["wave_id"],
        supersession_proof,
        redis=redis,
    )
    session.add(
        PTGImportWaveSupersession(
            predecessor_wave_id=predecessor_wave_id,
            successor_wave_id=request["wave_id"],
            recovery_basis="logical_preclaim_failure",
            recovery_evidence=witness.as_mapping(),
            recovery_evidence_canonical=canonical_json(witness.evidence_mapping()),
            recovery_evidence_sha256=witness.proof_digest,
            created_at=now,
        )
    )
    await session.flush()


__all__ = ["persist_admission_supersession", "validate_admission_supersession"]
