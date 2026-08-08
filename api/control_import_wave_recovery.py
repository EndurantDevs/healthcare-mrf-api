"""Atomic predecessor retirement orchestration for wave admission."""

from __future__ import annotations

from typing import Any

from api.control_import_wave_admission_rollback import (
    persist_admission_rollback_supersession,
    validate_admission_rollback_supersession,
)
from api.control_import_wave_supersession import (
    persist_admission_supersession,
    validate_admission_supersession,
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


def validate_admission_recovery_proofs(
    attestation: dict[str, Any],
    *,
    wave_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Validate the versioned logical and absent-admission proofs."""

    return (
        validate_admission_supersession(attestation, wave_id=wave_id),
        validate_admission_rollback_supersession(
            attestation,
            wave_id=wave_id,
        ),
    )


__all__ = [
    "persist_admission_recoveries",
    "validate_admission_recovery_proofs",
]
