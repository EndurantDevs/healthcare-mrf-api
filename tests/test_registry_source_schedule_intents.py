# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed clocks prove scope isolation and bounded scheduling intentions."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from process.registry_source_schedule_intents import (
    MAX_SOURCE_SCOPES,
    RegistrySourceRefreshScope,
    RegistrySourceScheduleError,
    build_registry_source_schedule_intents,
)


def _clock(iso="2026-10-07T12:00:00+00:00"):
    return datetime.fromisoformat(iso)


def _scope(**changes):
    return replace(RegistrySourceRefreshScope("cms", "commercial-mlr", "latest-verified"), **changes)


@pytest.mark.parametrize(
    "clock,due",
    [
        ("2026-10-04T23:59:59+00:00", "2026-09-28T03:00:00+00:00"),
        ("2026-10-05T02:59:59.999999+00:00", "2026-09-28T03:00:00+00:00"),
        ("2026-10-05T03:00:00+00:00", "2026-10-05T03:00:00+00:00"),
        ("2026-10-11T23:59:59+00:00", "2026-10-05T03:00:00+00:00"),
        ("2026-10-12T03:00:00+00:00", "2026-10-12T03:00:00+00:00"),
        ("2027-01-04T02:00:00+00:00", "2026-12-28T03:00:00+00:00"),
        ("2027-01-04T03:00:00+00:00", "2027-01-04T03:00:00+00:00"),
    ],
)
def test_cms_weekly_latest_elapsed_window(clock, due):
    batch = build_registry_source_schedule_intents([_scope(source_id="plan-finder")], _clock(clock))
    assert len(batch.intents) == 1 and batch.skipped == ()
    assert batch.intents[0].due_at == _clock(due)
    assert batch.intents[0].due_at.tzinfo is timezone.utc
    assert batch.intents[0].importer == "registry-source-check"


@pytest.mark.parametrize(
    "clock,due",
    [
        ("2026-10-01T02:59:59+00:00", "2026-09-01T03:00:00+00:00"),
        ("2026-10-01T03:00:00+00:00", "2026-10-01T03:00:00+00:00"),
        ("2026-10-31T23:59:59+00:00", "2026-10-01T03:00:00+00:00"),
        ("2027-01-01T02:59:59+00:00", "2026-12-01T03:00:00+00:00"),
        ("2027-01-01T03:00:00+00:00", "2027-01-01T03:00:00+00:00"),
        ("2024-02-29T23:59:59+00:00", "2024-02-01T03:00:00+00:00"),
        ("2024-03-01T02:59:59+00:00", "2024-02-01T03:00:00+00:00"),
        ("2024-03-01T03:00:00+00:00", "2024-03-01T03:00:00+00:00"),
    ],
)
@pytest.mark.parametrize("system,source_id", [("cms", "commercial-mlr"), ("naic", "company-directory")])
def test_mlr_and_naic_monthly_calendar_window(clock, due, system, source_id):
    scope = _scope(source_system=system, source_id=source_id)
    batch = build_registry_source_schedule_intents([scope], _clock(clock))
    assert len(batch.intents) == 1 and batch.intents[0].due_at == _clock(due)


@pytest.mark.parametrize("system,source_id", [("cms", "commercial-mlr"), ("naic", "company-directory")])
def test_first_representable_window_has_no_underflow(system, source_id):
    scope = _scope(source_system=system, source_id=source_id)
    before = build_registry_source_schedule_intents([scope], datetime(1, 1, 1, 2, 59, tzinfo=timezone.utc))
    assert before.intents == () and before.skipped[0].reason == "not_due"
    assert before.skipped[0].due_at is None
    due = build_registry_source_schedule_intents([scope], datetime(1, 1, 1, 3, tzinfo=timezone.utc))
    assert due.intents[0].due_at == datetime(1, 1, 1, 3, tzinfo=timezone.utc)
    final = build_registry_source_schedule_intents([scope], datetime.max.replace(tzinfo=timezone.utc))
    assert len(final.intents) == 1 and final.intents[0].due_at.year == 9999


def test_equivalent_offsets_and_day_boundary_normalize_to_utc():
    scope = _scope(source_id="plan-finder")
    utc = build_registry_source_schedule_intents([scope], _clock("2026-10-05T03:00:00+00:00"))
    offset = build_registry_source_schedule_intents([scope], _clock("2026-10-05T05:00:00+02:00"))
    assert offset == utc
    previous = build_registry_source_schedule_intents([scope], _clock("2026-10-04T21:59:59-05:00"))
    assert previous.intents[0].due_at == _clock("2026-09-28T03:00:00+00:00")
    assert previous.intents[0].dedup_key != utc.intents[0].dedup_key


@pytest.mark.parametrize(
    "elapsed,reason", [(-1, None), (0, "already_checked"), (1, "already_checked"), (604800, "already_checked")]
)
def test_check_time_comparison_includes_exact_due_boundary(elapsed, reason):
    due = _clock("2026-10-05T03:00:00+00:00")
    scope = _scope(source_id="plan-finder", last_checked_at=due + timedelta(seconds=elapsed))
    batch = build_registry_source_schedule_intents([scope], _clock())
    if reason is None:
        assert len(batch.intents) == 1 and batch.skipped == ()
    else:
        assert batch.intents == () and batch.skipped[0].reason == reason
        assert batch.skipped[0].due_at == due


def test_last_check_offset_normalizes_and_duplicate_observations_agree():
    first = _scope(last_checked_at=_clock("2026-10-05T04:00:00+01:00"))
    second = _scope(last_checked_at=_clock("2026-10-04T22:00:00-05:00"))
    assert first.last_checked_at.tzinfo is timezone.utc and first == second
    batch = build_registry_source_schedule_intents([first, second], _clock())
    assert batch.intents == () and len(batch.skipped) == 1
    assert batch.skipped[0].reason == "already_checked"


def test_same_window_replay_is_stable_and_next_window_is_distinct():
    scope = _scope(source_id="plan-finder")
    first = build_registry_source_schedule_intents([scope], _clock("2026-10-05T03:00:00+00:00"))
    repeated = build_registry_source_schedule_intents([scope], _clock("2026-10-11T23:59:00+00:00"))
    assert first == repeated
    checked = replace(scope, last_checked_at=_clock("2026-10-05T04:00:00+00:00"))
    next_window = build_registry_source_schedule_intents([checked], _clock("2026-10-12T03:00:00+00:00"))
    assert next_window.intents[0].dedup_key != first.intents[0].dedup_key
    old_check = replace(scope, last_checked_at=_clock("2026-10-04T23:00:00+00:00"))
    assert build_registry_source_schedule_intents([old_check], _clock()).intents == first.intents


def test_domain_bound_key_has_stable_canonical_vector():
    batch = build_registry_source_schedule_intents([_scope()], _clock())
    assert batch.intents[0].dedup_key == "204d75e4ad599f3bf7bb28c8f1bf7c218d60299ab041cfd15f452b7aeb86c174"


def test_explicit_scope_policy_and_optional_year_never_collide():
    scopes = [
        _scope(),
        _scope(reporting_year=2024),
        _scope(reporting_year=2025),
        _scope(edition_policy="fixed-year", reporting_year=2024),
        _scope(source_id="plan-finder"),
        _scope(source_system="naic", source_id="company-directory"),
    ]
    batch = build_registry_source_schedule_intents(scopes, _clock())
    assert len(batch.intents) == 6 and len({intent.dedup_key for intent in batch.intents}) == 6
    assert {(intent.edition_policy, intent.reporting_year) for intent in batch.intents} >= {
        ("latest-verified", None),
        ("latest-verified", 2024),
        ("latest-verified", 2025),
        ("fixed-year", 2024),
    }
    assert build_registry_source_schedule_intents(list(reversed(scopes)), _clock()) == batch


def test_unavailable_and_unsupported_do_not_block_ready_sources():
    scopes = [
        _scope(source_system="naic", source_id="company-directory", available=False),
        _scope(),
        _scope(source_id="plan-finder"),
        _scope(source_system="manual", source_id="company"),
        _scope(source_system="naic", source_id="plan-finder"),
    ]
    batch = build_registry_source_schedule_intents(scopes, _clock())
    assert {intent.source_id for intent in batch.intents} == {"commercial-mlr", "plan-finder"}
    assert {(skip.source_system, skip.source_id, skip.reason) for skip in batch.skipped} == {
        ("naic", "company-directory", "unavailable"),
        ("manual", "company", "unsupported"),
        ("naic", "plan-finder", "unsupported"),
    }


@pytest.mark.parametrize(
    "changes",
    [{"available": False}, {"last_checked_at": _clock()}, {"last_checked_at": _clock("2026-01-01T00:00:00+00:00")}],
)
def test_conflicting_duplicate_observations_reject_entire_batch(changes):
    with pytest.raises(RegistrySourceScheduleError):
        build_registry_source_schedule_intents([_scope(source_id="plan-finder"), _scope(), _scope(**changes)], _clock())


def test_exact_duplicates_and_batch_limits():
    duplicate = build_registry_source_schedule_intents([_scope()] * MAX_SOURCE_SCOPES, _clock())
    assert len(duplicate.intents) == 1 and duplicate.skipped == ()
    scopes = [_scope(edition_policy="fixed-year", reporting_year=1900 + index) for index in range(MAX_SOURCE_SCOPES)]
    bounded = build_registry_source_schedule_intents(scopes, _clock())
    assert (
        len(bounded.intents) == MAX_SOURCE_SCOPES
        and len({intent.dedup_key for intent in bounded.intents}) == MAX_SOURCE_SCOPES
    )
    with pytest.raises(RegistrySourceScheduleError):
        build_registry_source_schedule_intents([_scope()] * (MAX_SOURCE_SCOPES + 1), _clock())
    assert build_registry_source_schedule_intents([], _clock()).intents == ()
    assert build_registry_source_schedule_intents((), _clock()).skipped == ()


@pytest.mark.parametrize(
    "scopes", [None, "cms", {"source_system": "cms"}, {_scope()}, iter([_scope()]), [None], [{}], ["cms"]]
)
def test_malformed_batch_container_or_member_is_rejected(scopes):
    with pytest.raises(RegistrySourceScheduleError):
        build_registry_source_schedule_intents(scopes, _clock())


@pytest.mark.parametrize(
    "changes",
    [
        {"source_system": ""},
        {"source_system": " cms"},
        {"source_system": "a" * 65},
        {"source_system": 1},
        {"source_id": None},
        {"source_id": "bad\nsource"},
        {"source_id": "a" * 129},
        {"edition_policy": "latest"},
        {"edition_policy": None},
        {"edition_policy": []},
        {"edition_policy": "fixed-year"},
        {"reporting_year": True},
        {"reporting_year": "2024"},
        {"reporting_year": 1899},
        {"reporting_year": 10000},
        {"available": 1},
        {"available": "true"},
        {"last_checked_at": datetime(2026, 10, 5)},
        {"last_checked_at": "2026-10-05T03:00:00Z"},
    ],
)
def test_scope_trust_boundary_rejects_malformed_types(changes):
    with pytest.raises(RegistrySourceScheduleError):
        _scope(**changes)


@pytest.mark.parametrize("now", [None, True, "2026-10-07T00:00:00Z", datetime(2026, 10, 7)])
def test_clock_must_be_explicitly_aware_datetime(now):
    with pytest.raises(RegistrySourceScheduleError):
        build_registry_source_schedule_intents([_scope()], now)


def test_utc_normalization_overflow_is_typed_invalid():
    underflow = datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    overflow = datetime.max.replace(tzinfo=timezone(timedelta(hours=-1)))
    for clock in (underflow, overflow):
        with pytest.raises(RegistrySourceScheduleError):
            build_registry_source_schedule_intents([_scope()], clock)
        with pytest.raises(RegistrySourceScheduleError):
            _scope(last_checked_at=clock)


def test_scopes_and_batch_outputs_are_immutable():
    scope = _scope()
    batch = build_registry_source_schedule_intents([scope], _clock())
    with pytest.raises(FrozenInstanceError):
        scope.available = False
    with pytest.raises(FrozenInstanceError):
        batch.intents[0].reporting_year = 2025
    with pytest.raises(FrozenInstanceError):
        batch.intents = ()
    assert isinstance(batch.intents, tuple) and isinstance(batch.skipped, tuple)


def test_builder_revalidates_forged_scope_before_any_intent():
    malformed = _scope()
    object.__setattr__(malformed, "available", "true")
    with pytest.raises(RegistrySourceScheduleError):
        build_registry_source_schedule_intents([_scope(source_id="plan-finder"), malformed], _clock())


def test_revalidation_never_mutates_caller_scope():
    scope = _scope(source_id="plan-finder")
    offset_check = _clock("2026-10-04T23:00:00+02:00")
    object.__setattr__(scope, "last_checked_at", offset_check)
    batch = build_registry_source_schedule_intents([scope], _clock())
    assert len(batch.intents) == 1 and scope.last_checked_at is offset_check
