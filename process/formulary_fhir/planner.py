# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic weekday delta/reconciliation decisions per DrugPlan alias."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass
from enum import StrEnum


DELTA_OVERLAP = dt.timedelta(minutes=5)
ROLLING_RECONCILIATION_BUCKETS = 5


class AliasSyncDecision(StrEnum):
    REUSE = "reuse"
    DELTA = "delta"
    FULL = "full"


@dataclass(frozen=True)
class AliasSyncObservation:
    source_plan_identifier: str
    exact_count: int
    prior_count: int | None
    delta_ids: frozenset[str]
    prior_membership_ids: frozenset[str]
    rolling_reconciliation_due: bool = False


def reconciliation_bucket(source_plan_identifier: str) -> int:
    """Map an alias deterministically into one of five business-day buckets."""

    digest = hashlib.sha256(source_plan_identifier.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % ROLLING_RECONCILIATION_BUCKETS


def is_rolling_reconciliation_due(
    source_plan_identifier: str,
    *,
    business_day_ordinal: int,
) -> bool:
    """Return whether the alias requires its rolling full reconciliation."""

    return reconciliation_bucket(source_plan_identifier) == (
        business_day_ordinal % ROLLING_RECONCILIATION_BUCKETS
    )


def delta_window_start(previous_cutoff: dt.datetime) -> dt.datetime:
    """Return the overlapped lower bound for a delta request."""

    return previous_cutoff - DELTA_OVERLAP


def decide_alias_sync(observation: AliasSyncObservation) -> AliasSyncDecision:
    """Choose reuse, delta update, or a full rebuild for one alias."""

    if observation.exact_count < 0:
        raise ValueError("alias exact count must be non-negative")
    if observation.rolling_reconciliation_due or observation.prior_count is None:
        return AliasSyncDecision.FULL
    if observation.exact_count != observation.prior_count:
        return AliasSyncDecision.FULL
    if not observation.delta_ids:
        return AliasSyncDecision.REUSE
    if observation.delta_ids.issubset(observation.prior_membership_ids):
        return AliasSyncDecision.DELTA
    return AliasSyncDecision.FULL


@dataclass
class AdaptiveAliasConcurrency:
    """Bounded 1/2/4/8 controller that reacts conservatively to throttling."""

    configured: int = 4
    current: int = 4
    clean_windows: int = 0

    def __post_init__(self) -> None:
        allowed = (1, 2, 4, 8)
        if self.configured not in allowed:
            raise ValueError("alias concurrency must be one of 1, 2, 4, or 8")
        self.current = min(self.current, self.configured)
        if self.current not in allowed:
            raise ValueError("current alias concurrency must be one of 1, 2, 4, or 8")

    def record_throttling(self) -> int:
        """Halve concurrency immediately after upstream throttling."""

        self.current = max(1, self.current // 2)
        self.clean_windows = 0
        return self.current

    def record_clean_window(self, *, recovery_windows: int = 3) -> int:
        """Recover one concurrency step after consecutive clean windows."""

        self.clean_windows += 1
        if self.clean_windows >= recovery_windows and self.current < self.configured:
            self.current = min(self.configured, self.current * 2)
            self.clean_windows = 0
        return self.current
