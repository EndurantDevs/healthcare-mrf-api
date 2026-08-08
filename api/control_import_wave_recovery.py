"""Atomic predecessor retirement orchestration for wave admission."""

from __future__ import annotations

from typing import Any

from api.control_import_wave_attestation import (
    MATERIALIZED_PRECLAIM_ATTESTATION_VERSION,
    ROLLBACK_ATTESTATION_VERSION,
)
from api.control_import_wave_admission_rollback import (
    persist_admission_rollback_supersession,
    validate_admission_rollback_supersession,
)
from api.control_import_wave_supersession import (
    persist_admission_supersession,
    validate_admission_supersession,
)
from api.control_import_wave_materialized_preclaim import (
    persist_materialized_preclaim_supersession,
    validate_materialized_preclaim_supersession,
)
from process.ptg_wave_admission_rollback_supersession import (
    PTGWaveAdmissionRollbackConflict,
)
from process.ptg_wave_admission_rollback_supersession_runtime import (
    find_admission_retirement_collision,
)


async def persist_admission_recoveries(
    session: Any,
    request: dict[str, Any],
    *,
    now: Any,
    redis: Any,
) -> None:
    """Reject retired identities, then persist both signed retirements."""

    collision = await find_admission_retirement_collision(session, request)
    if collision is not None:
        raise PTGWaveAdmissionRollbackConflict(
            "import wave admission identity is permanently retired"
        )
    await persist_admission_supersession(
        session,
        request,
        now=now,
        redis=redis,
    )
    await persist_admission_rollback_supersession(
        session,
        request,
        now=now,
        redis=redis,
    )
    await persist_materialized_preclaim_supersession(
        session,
        request,
        now=now,
        redis=redis,
    )


def project_admission_recovery_proofs(
    attestation: dict[str, Any],
    *,
    wave_id: str,
) -> dict[str, dict[str, Any] | None]:
    """Validate and project only proofs allowed by the schema version."""

    recovery_proof_map = {
        "supersession": validate_admission_supersession(
            attestation,
            wave_id=wave_id,
        )
    }
    if attestation["schema_version"] == ROLLBACK_ATTESTATION_VERSION:
        recovery_proof_map["admission_rollback_supersession"] = (
            validate_admission_rollback_supersession(
                attestation,
                wave_id=wave_id,
            )
        )
    if (
        attestation["schema_version"]
        == MATERIALIZED_PRECLAIM_ATTESTATION_VERSION
    ):
        recovery_proof_map["materialized_preclaim_supersession"] = (
            validate_materialized_preclaim_supersession(
                attestation,
                wave_id=wave_id,
            )
        )
    return recovery_proof_map


__all__ = [
    "persist_admission_recoveries",
    "project_admission_recovery_proofs",
]
