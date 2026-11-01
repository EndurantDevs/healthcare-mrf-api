# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capped-count and uncounted partitions retain exact leaf proof requirements."""

import datetime
import json

import pytest

from process.provider_directory_time_partition import (
    CountKind,
    CountObservation,
    PartitionPlan,
    PartitionPlanError,
    PlanStatus,
    WindowState,
)

HOUR = datetime.timedelta(hours=1)
START = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


def new_plan(*, hours=4):
    return PartitionPlan.create(START, START + hours * HOUR, ceiling=3, minimum_width=HOUR)


def split_plan(observation):
    plan = new_plan()
    plan.observe_count("root", observation)
    return plan


@pytest.mark.parametrize("lower_bound", [True, False, 0, -1, 1.0, "4", None])
def test_overflow_requires_a_positive_integer_lower_bound(lower_bound):
    with pytest.raises(PartitionPlanError, match="positive integer"):
        CountObservation.overflow(lower_bound)


def test_overflow_and_timeout_remain_distinct_from_exact_counts():
    assert CountObservation.overflow(4).kind is CountKind.OVERFLOW
    assert CountObservation.overflow(4) != CountObservation.exact(4)
    assert CountObservation.split().count is None
    assert CountObservation.split().kind is CountKind.SPLIT
    with pytest.raises(PartitionPlanError, match="error detail"):
        CountObservation(CountKind.OVERFLOW, count=4, detail="unexpected")


@pytest.mark.parametrize("reason", ["auth_required", "http_429", "http_503", "unknown", "", None])
def test_uncounted_split_does_not_admit_arbitrary_errors(reason):
    with pytest.raises(PartitionPlanError, match="timeout reason"):
        CountObservation.split(reason)


def test_uncounted_split_rejects_an_invented_count():
    with pytest.raises(PartitionPlanError, match="no count"):
        CountObservation(CountKind.SPLIT, count=4, detail="timeout")


@pytest.mark.parametrize("observation", [CountObservation.overflow(4), CountObservation.split()])
def test_split_keeps_uncounted_parent_and_contiguous_children(observation):
    plan = split_plan(observation)
    root = plan.windows["root"]
    left = plan.windows["root.0"]
    right = plan.windows["root.1"]
    assert root.count is None and root.state is WindowState.SPLIT
    assert left.start == root.start and left.end == right.start and right.end == root.end
    assert left.end == START + 2 * HOUR
    assert plan.status is PlanStatus.COUNTING
    checkpoint = plan.to_json()
    plan.observe_count("root", observation)
    assert plan.to_json() == checkpoint
    assert PartitionPlan.from_json(checkpoint).to_json() == checkpoint


@pytest.mark.parametrize("observation", [CountObservation.overflow(4), CountObservation.split()])
def test_minimum_width_failure_roundtrips_with_its_evidence(observation):
    plan = new_plan(hours=1)
    plan.observe_count("root", observation)
    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == "minimum_width_overflow"
    assert set(plan.windows) == {"root"}
    assert PartitionPlan.from_json(plan.to_json()).to_json() == plan.to_json()


@pytest.mark.parametrize("observation", [CountObservation.overflow(4), CountObservation.split()])
def test_changed_split_observation_is_terminal(observation):
    plan = split_plan(observation)
    plan.observe_count("root", CountObservation.exact(4))
    assert plan.failure.code == "contradictory_count"


def test_overflow_lower_bound_must_exceed_the_configured_ceiling():
    plan = new_plan()
    with pytest.raises(PartitionPlanError, match="exceed the ceiling"):
        plan.observe_count("root", CountObservation.overflow(3))
    assert plan.windows["root"].state is WindowState.NEEDS_COUNT


@pytest.mark.parametrize("observation", [CountObservation.overflow(4), CountObservation.split()])
def test_exact_leaves_and_two_matching_passes_are_still_required(observation):
    plan = split_plan(observation)
    for window_id in ("root.0", "root.1"):
        plan.observe_count(window_id, CountObservation.exact(2))
    assert plan.status is PlanStatus.ACQUIRING
    for window_id in ("root.0", "root.1"):
        resources = [{"id": window_id + suffix} for suffix in ("a", "b")]
        plan.record_pass(window_id, 1, resources, complete=True)
        assert plan.status is PlanStatus.ACQUIRING
        plan.record_pass(window_id, 2, resources, complete=True)
    assert plan.status is PlanStatus.SUCCEEDED
    assert PartitionPlan.from_json(plan.to_json()).to_json() == plan.to_json()


@pytest.mark.parametrize(
    "observation,code",
    [(CountObservation.error("timeout"), "count_error"), (CountObservation.unknown(), "count_unknown")],
)
def test_existing_error_and_unknown_observations_still_fail(observation, code):
    plan = new_plan()
    plan.observe_count("root", observation)
    assert plan.status is PlanStatus.FAILED and plan.failure.code == code


def test_exact_leaf_totals_cannot_contradict_a_certified_overflow():
    plan = split_plan(CountObservation.overflow(4))
    plan.observe_count("root.0", CountObservation.exact(1))
    plan.observe_count("root.1", CountObservation.exact(2))
    assert plan.failure.code == "overflow_count_mismatch"
    assert PartitionPlan.from_json(plan.to_json()).to_json() == plan.to_json()


@pytest.mark.parametrize(
    "field,value", [("overflow_lower_bound", True), ("overflow_lower_bound", 3), ("split_reason", "http_403")]
)
def test_checkpoint_rejects_forged_split_evidence(field, value):
    checkpoint = split_plan(CountObservation.overflow(4)).to_dict()
    checkpoint["windows"][0][field] = value
    with pytest.raises(PartitionPlanError):
        PartitionPlan.from_dict(checkpoint)


@pytest.mark.parametrize(
    "parent_update_by_field,missing_child,shift_midpoint",
    [
        ({"split_reason": "timeout"}, False, False),
        ({"count": 4}, False, False),
        ({"passes": {"1": {}}}, False, False),
        ({"state": "ready"}, False, False),
        ({}, True, False),
        ({}, False, True),
        ({"overflow_lower_bound": 5}, False, False),
    ],
    ids=[
        "both-markers",
        "exact-parent",
        "passes-parent",
        "ready-parent",
        "missing-child",
        "shift-midpoint",
        "false-lower-bound",
    ],
)
def test_checkpoint_rejects_uncounted_parent_or_geometry_tampering(
    parent_update_by_field, missing_child, shift_midpoint
):
    plan = split_plan(CountObservation.overflow(4))
    plan.observe_count("root.0", CountObservation.exact(2))
    plan.observe_count("root.1", CountObservation.exact(2))
    checkpoint_by_field = plan.to_dict()
    checkpoint_by_field["windows"][0].update(parent_update_by_field)
    if missing_child:
        checkpoint_by_field["windows"].pop()
    if shift_midpoint:
        checkpoint_by_field["windows"][1]["end"] = "2026-01-01T01:00:00.000000Z"
        checkpoint_by_field["windows"][2]["start"] = "2026-01-01T01:00:00.000000Z"
    with pytest.raises(PartitionPlanError):
        PartitionPlan.from_dict(checkpoint_by_field)


def test_historical_checkpoint_bytes_do_not_gain_new_markers():
    checkpoint_by_field = {
        "version": 2,
        "config": {
            "ceiling": 3,
            "minimum_width_microseconds": 3600000000,
            "boundary_precision_microseconds": 1,
            "volatile_metadata_paths": [],
        },
        "windows": [
            {
                "window_id": "root",
                "start": "2026-01-01T00:00:00.000000Z",
                "end": "2026-01-01T04:00:00.000000Z",
                "state": "needs_count",
                "count": None,
                "passes": {},
            }
        ],
        "failure": None,
    }
    historical_bytes = json.dumps(checkpoint_by_field, sort_keys=True, separators=(",", ":"))
    assert PartitionPlan.from_json(historical_bytes).to_json() == historical_bytes
