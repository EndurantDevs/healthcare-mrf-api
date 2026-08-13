# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Test helpers for settling bounded PTG lifecycle deferrals."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred


async def settle_lifecycle_outcomes(
    outcomes: Sequence[object],
    *,
    replay: Callable[[], Awaitable[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Replay only authenticated bounded deferrals after contenders settle."""

    reports: list[dict[str, Any]] = []
    deferred_count = 0
    for outcome in outcomes:
        if isinstance(outcome, PTG2LifecycleLockDeferred):
            assert outcome.retryable is True
            deferred_count += 1
        elif isinstance(outcome, BaseException):
            raise outcome
        else:
            reports.append(outcome)
    for _deferred_ordinal in range(deferred_count):
        reports.append(await replay())
    assert len(reports) == len(outcomes)
    return reports


__all__ = ["settle_lifecycle_outcomes"]
