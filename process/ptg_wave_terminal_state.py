"""Pure final-state reduction for exact PTG waves."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.ptg_wave_admission_fence import PTG_WAVE_TERMINAL_STATES
from process.ptg_wave_state import PTGWaveStateConflict


def derive_terminal_state(wave: Any, outcomes: list[Any]) -> str:
    """Derive the only final state supported by exact ordered outcomes."""

    if (
        len(outcomes) != wave.intent_count
        or [outcome.ordinal for outcome in outcomes]
        != list(range(wave.intent_count))
    ):
        raise PTGWaveStateConflict(
            "final state requires every exact outcome ordinal"
        )
    statuses = {str(outcome.status) for outcome in outcomes}
    if not statuses or not statuses.issubset(PTG_WAVE_TERMINAL_STATES):
        raise PTGWaveStateConflict(
            "final state requires only terminal exact outcomes"
        )
    if wave.failure_receipt_digest is not None and statuses != {"dead_letter"}:
        raise PTGWaveStateConflict(
            "unclaimed failure may only finalize as all dead letter"
        )
    if statuses == {"succeeded"}:
        return "succeeded"
    if "dead_letter" in statuses:
        return "dead_letter"
    if "canceled" in statuses:
        return "canceled"
    return "failed"
