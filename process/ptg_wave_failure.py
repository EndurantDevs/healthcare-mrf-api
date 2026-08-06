"""Fail-closed recovery and exact all-N failure handling for PTG waves."""

from __future__ import annotations

import sys
from typing import Any

from db.models import db
from process.ptg_wave_failure_kubernetes import (
    _verify_failure_kubernetes,
    _verify_kubernetes_absence,
    _verify_preclaim_kubernetes_failure,
)
from process.ptg_wave_failure_receipts import (
    _claimed_prestart_failure_receipt,
    _require_claimed_prestart_failure_receipt,
    _require_failure_receipt,
    _require_unclaimed_failure_receipt,
    read_only_recovery_plan,
)
from process.ptg_wave_failure_snapshots import (
    _is_prestart_run_pristine,
    _started_claim_ordinals,
    _worker_start_event_ordinals,
    snapshot_claimed_prestart_dead_letter_outcomes as _snapshot_claimed_prestart,
    snapshot_unclaimed_dead_letter_outcomes as _snapshot_unclaimed,
)
from process.ptg_wave_failure_terminal import (
    verify_claimed_prestart_terminal_eligibility,
    verify_unclaimed_dead_letter_terminal_eligibility,
)
from process.ptg_wave_failure_types import (
    CLAIMED_PRESTART_FAILURE_REASON,
    CLAIMED_PRESTART_FAILURE_SCHEMA,
    FAILURE_REASONS,
    FAILURE_SCHEMA,
    PTGWaveFailureConflict,
    PTGWaveReadOnlyRecovery,
    _claimed_ordinals_digest,
    _dead_letter_record,
    _digest,
    _outcomes_digest,
    _require_mapping,
    _rows_by_ordinal,
    _single_outcome_digest,
    _unclaimed_ordinals_digest,
    is_claimed_prestart_failure_receipt,
)
from process.ptg_wave_failure_validation import (
    _expected_redis_ready_slots,
    _expected_redis_release_mapping,
    _ordinal_set,
    _verify_failure_redis,
    _verify_linkage,
)
from process.ptg_wave_state import _locked_wave, _transition


_FAILURE_SCHEMA = FAILURE_SCHEMA
_CLAIMED_PRESTART_FAILURE_SCHEMA = CLAIMED_PRESTART_FAILURE_SCHEMA
_CLAIMED_PRESTART_FAILURE_REASON = CLAIMED_PRESTART_FAILURE_REASON
_FAILURE_REASONS = FAILURE_REASONS
_outcome_digest = _single_outcome_digest
_prestart_run_is_pristine = _is_prestart_run_pristine


async def snapshot_unclaimed_dead_letter_outcomes(
    wave_id: str,
    *,
    failure_receipt: object,
) -> str:
    """Atomically dead-letter all N unclaimed runs and enter linkage wait."""

    return await _snapshot_unclaimed(
        sys.modules[__name__],
        wave_id,
        failure_receipt=failure_receipt,
    )


async def snapshot_claimed_prestart_dead_letter_outcomes(
    wave_id: str,
    *,
    kubernetes_evidence: object,
    redis_evidence: object,
) -> str:
    """Atomically close the claim-commit/import-start crash boundary."""

    return await _snapshot_claimed_prestart(
        sys.modules[__name__],
        wave_id,
        kubernetes_evidence=kubernetes_evidence,
        redis_evidence=redis_evidence,
    )


verify_claimed_prestart_dead_letter_terminal_eligibility = (
    verify_claimed_prestart_terminal_eligibility
)


__all__ = [
    "PTGWaveFailureConflict",
    "PTGWaveReadOnlyRecovery",
    "is_claimed_prestart_failure_receipt",
    "read_only_recovery_plan",
    "snapshot_claimed_prestart_dead_letter_outcomes",
    "snapshot_unclaimed_dead_letter_outcomes",
    "verify_claimed_prestart_terminal_eligibility",
    "verify_claimed_prestart_dead_letter_terminal_eligibility",
    "verify_unclaimed_dead_letter_terminal_eligibility",
]
