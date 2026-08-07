"""Shared dead-letter persistence for exact-wave failure reducers."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from db.models import PTGImportWaveOutcome
from process.ptg_wave_failure_types import (
    _dead_letter_record,
    _outcomes_digest,
    _single_outcome_digest,
)


@dataclass(frozen=True)
class DeadLetterSnapshot:
    """One fully validated locked snapshot ready for mutation."""

    session: Any
    wave: Any
    wave_id: str
    intents: list[Any]
    runs: list[Any]
    receipt: dict[str, Any]
    receipt_digest: str
    is_claimed_prestart: bool


async def persist_dead_letter_snapshot(
    facade: ModuleType, snapshot: DeadLetterSnapshot
) -> str:
    """Persist all-N outcomes after every validation has passed."""

    recorded_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    _dead_letter_runs(
        snapshot.runs,
        recorded_at,
        is_claimed_prestart=snapshot.is_claimed_prestart,
    )
    dead_letter_records = [
        _dead_letter_record(intent) for intent in snapshot.intents
    ]
    for dead_letter_record in dead_letter_records:
        snapshot.session.add(
            PTGImportWaveOutcome(
                **dead_letter_record,
                wave_id=snapshot.wave_id,
                outcome_digest=_single_outcome_digest(dead_letter_record),
                recorded_at=recorded_at,
            )
        )
    outcomes_digest = _outcomes_digest(dead_letter_records)
    await facade._transition(
        snapshot.session,
        snapshot.wave,
        "awaiting_linkage",
        values={
            "outcomes_digest": outcomes_digest,
            "failure_receipt": snapshot.receipt,
            "failure_receipt_digest": snapshot.receipt_digest,
        },
    )
    return outcomes_digest


def _dead_letter_runs(
    runs: list[Any],
    recorded_at: dt.datetime,
    *,
    is_claimed_prestart: bool,
) -> None:
    for run in runs:
        run.status = "dead_letter"
        run.finished_at = recorded_at
        if is_claimed_prestart:
            run.phase_detail = (
                "PTG exact-wave Pods stopped before import execution"
            )
            run.heartbeat_at = recorded_at
            run.progress = {
                "unit": "run", "total": 1, "done": 0, "pct": 0,
                "message": "dead letter",
            }
            run.error = {
                "code": "ptg_exact_wave_claimed_prestart_failure",
                "retryable": False,
            }
        else:
            run.phase_detail = "PTG exact-wave stopped before a worker claim"
            run.error = {
                "code": "ptg_exact_wave_unclaimed_failure",
                "retryable": False,
            }
