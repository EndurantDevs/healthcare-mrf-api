# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt

from process.formulary_fhir.planner import (
    AdaptiveAliasConcurrency,
    AliasSyncDecision,
    AliasSyncObservation,
    decide_alias_sync,
    delta_window_start,
    is_rolling_reconciliation_due,
    reconciliation_bucket,
)
from process.formulary_fhir.synchronizer import business_day_ordinal


def _observation(**overrides):
    observation_by_field = {
        "source_plan_identifier": "alias-a",
        "exact_count": 2,
        "prior_count": 2,
        "delta_ids": frozenset(),
        "prior_membership_ids": frozenset({"a", "b"}),
        "rolling_reconciliation_due": False,
    }
    observation_by_field.update(overrides)
    return AliasSyncObservation(**observation_by_field)


def test_empty_delta_with_equal_count_reuses_alias_version():
    assert decide_alias_sync(_observation()) == AliasSyncDecision.REUSE


def test_known_equal_count_updates_use_delta_but_replacement_uses_full():
    assert decide_alias_sync(
        _observation(delta_ids=frozenset({"a"}))
    ) == AliasSyncDecision.DELTA
    assert decide_alias_sync(
        _observation(delta_ids=frozenset({"new"}))
    ) == AliasSyncDecision.FULL


def test_count_change_or_rolling_reconciliation_forces_full():
    assert decide_alias_sync(_observation(exact_count=3)) == AliasSyncDecision.FULL
    assert decide_alias_sync(
        _observation(rolling_reconciliation_due=True)
    ) == AliasSyncDecision.FULL


def test_five_minute_overlap_and_business_day_buckets_are_deterministic():
    cutoff = dt.datetime(2026, 8, 6, 12, tzinfo=dt.UTC)
    assert delta_window_start(cutoff) == cutoff - dt.timedelta(minutes=5)
    friday = business_day_ordinal(dt.date(2026, 8, 7))
    monday = business_day_ordinal(dt.date(2026, 8, 10))
    assert monday == friday + 1
    alias = "alias-a"
    due_days = [
        ordinal
        for ordinal in range(10, 15)
        if is_rolling_reconciliation_due(alias, business_day_ordinal=ordinal)
    ]
    assert due_days == [10 + reconciliation_bucket(alias)]


def test_adaptive_concurrency_halves_on_throttle_and_recovers_gradually():
    controller = AdaptiveAliasConcurrency(configured=8, current=8)

    assert controller.record_throttling() == 4
    assert controller.record_throttling() == 2
    assert controller.record_clean_window() == 2
    assert controller.record_clean_window() == 2
    assert controller.record_clean_window() == 4
