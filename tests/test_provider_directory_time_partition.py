# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import json

import pytest

from process.provider_directory_time_partition import (
    CountObservation,
    PartitionPlan,
    PartitionPlanError,
    PlanStatus,
    fingerprint_resource,
    parse_utc_instant,
)


UTC = datetime.UTC
HOUR = datetime.timedelta(hours=1)


def instant(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 1, 1, hour, tzinfo=UTC)


def resource(resource_id: str, *, updated: str = "first", active: bool = True) -> dict:
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "active": active,
        "meta": {"lastUpdated": updated, "versionId": updated},
    }


def plan_for_two_windows() -> PartitionPlan:
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(3))
    plan.observe_count("root.0", CountObservation.exact(1))
    plan.observe_count("root.1", CountObservation.exact(1))
    return plan


def record_twin_passes(plan: PartitionPlan, window_id: str, resources: list[dict]) -> None:
    plan.record_pass(window_id, 1, resources, complete=True)
    plan.record_pass(window_id, 2, resources, complete=True)


@pytest.mark.parametrize(
    "start,end",
    [
        (datetime.datetime(2026, 1, 1), instant(1)),
        (instant(0), datetime.datetime(2026, 1, 1, 2, tzinfo=datetime.timezone(HOUR))),
        (instant(1), instant(1)),
        (instant(2), instant(1)),
    ],
)
def test_plan_requires_ordered_explicit_utc_instants(start, end):
    with pytest.raises(PartitionPlanError):
        PartitionPlan.create(start, end, ceiling=10, minimum_width=HOUR)


def test_parse_utc_instant_rejects_non_utc_offset():
    with pytest.raises(PartitionPlanError, match="explicitly UTC"):
        parse_utc_instant("2026-01-01T01:00:00+01:00")


def test_recursive_split_produces_ordered_non_overlapping_half_open_windows():
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(8))
    plan.observe_count("root.0", CountObservation.exact(4))
    plan.observe_count("root.1", CountObservation.exact(2))
    plan.observe_count("root.0.0", CountObservation.exact(2))
    plan.observe_count("root.0.1", CountObservation.exact(2))

    leaves = plan.leaf_windows()

    assert [(entry.start, entry.end) for entry in leaves] == [
        (instant(0), instant(1)),
        (instant(1), instant(2)),
        (instant(2), instant(4)),
    ]
    assert all(left.end <= right.start for left, right in zip(leaves, leaves[1:]))
    assert plan.status is PlanStatus.ACQUIRING


@pytest.mark.parametrize(
    "observation,code",
    [
        (CountObservation.error("timeout"), "count_error"),
        (CountObservation.unknown(), "count_unknown"),
    ],
)
def test_non_exact_count_terminally_fails(observation, code):
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)

    plan.observe_count("root", observation)

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == code


def test_contradictory_count_terminally_fails_after_idempotent_replay():
    plan = PartitionPlan.create(instant(0), instant(2), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))
    plan.observe_count("root", CountObservation.exact(2))

    plan.observe_count("root", CountObservation.exact(1))

    assert plan.failure.code == "contradictory_count"


@pytest.mark.parametrize("bad_count", [-1, True, 1.5])
def test_exact_count_rejects_invalid_values(bad_count):
    with pytest.raises(PartitionPlanError):
        CountObservation.exact(bad_count)


def test_over_ceiling_unsplittable_window_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)

    plan.observe_count("root", CountObservation.exact(3))

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == "minimum_width_overflow"


def test_window_below_two_minimum_widths_cannot_create_invalid_children():
    plan = PartitionPlan.create(
        instant(0),
        instant(0) + datetime.timedelta(minutes=90),
        ceiling=2,
        minimum_width=HOUR,
    )

    plan.observe_count("root", CountObservation.exact(3))

    assert plan.failure.code == "minimum_width_overflow"
    assert list(plan.windows) == ["root"]


def test_checkpoint_is_deterministic_json_and_round_trips():
    plan = plan_for_two_windows()
    plan.record_pass("root.1", 1, [resource("right")], complete=True)
    checkpoint = plan.to_json()

    restored = PartitionPlan.from_json(checkpoint)

    assert restored.to_json() == checkpoint
    assert json.loads(checkpoint)["windows"][0]["start"].endswith("Z")
    assert restored.status is PlanStatus.ACQUIRING


def test_checkpoint_rejects_noncontiguous_leaf_windows():
    checkpoint = plan_for_two_windows().to_dict()
    checkpoint["windows"][1]["end"] = "2026-01-01T01:00:00.000000Z"

    with pytest.raises(PartitionPlanError, match="contiguous"):
        PartitionPlan.from_dict(checkpoint)


def test_checkpoint_rejects_pass_numbers_that_could_manufacture_success():
    checkpoint = PartitionPlan.create(
        instant(0), instant(1), ceiling=2, minimum_width=HOUR
    )
    checkpoint.observe_count("root", CountObservation.exact(0))
    checkpoint_fields = checkpoint.to_dict()
    checkpoint_fields["windows"][0]["passes"] = {"3": {}, "4": {}}

    with pytest.raises(PartitionPlanError, match="pass sequence"):
        PartitionPlan.from_dict(checkpoint_fields)


def test_success_requires_two_complete_passes_for_every_leaf():
    plan = plan_for_two_windows()
    record_twin_passes(plan, "root.0", [resource("left")])
    plan.record_pass("root.1", 1, [resource("right")], complete=True)

    assert plan.status is PlanStatus.ACQUIRING

    plan.record_pass("root.1", 2, [resource("right")], complete=True)

    assert plan.status is PlanStatus.SUCCEEDED


@pytest.mark.parametrize(
    "complete,bounded,code",
    [
        (False, False, "incomplete_window"),
        (True, True, "bounded_window"),
    ],
)
def test_incomplete_or_bounded_window_terminally_fails(complete, bounded, code):
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(0))

    plan.record_pass("root", 1, [], complete=complete, bounded=bounded)

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == code


def test_exact_count_must_reconcile_with_unique_resources():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))

    plan.record_pass("root", 1, [resource("only-one")], complete=True)

    assert plan.failure.code == "count_mismatch"


def test_duplicate_resource_id_within_window_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))

    plan.record_pass("root", 1, [resource("same"), resource("same")], complete=True)

    assert plan.failure.code == "duplicate_resource_id"


def test_duplicate_resource_id_across_windows_terminally_fails():
    plan = plan_for_two_windows()
    plan.record_pass("root.0", 1, [resource("same")], complete=True)

    plan.record_pass("root.1", 1, [resource("same")], complete=True)

    assert plan.failure.code == "duplicate_resource_id"


def test_twin_pass_fingerprint_ignores_configured_volatile_paths():
    plan = PartitionPlan.create(
        instant(0),
        instant(1),
        ceiling=2,
        minimum_width=HOUR,
        volatile_metadata_paths=("/meta/lastUpdated", "/meta/versionId"),
    )
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("stable", updated="first")], complete=True)

    plan.record_pass("root", 2, [resource("stable", updated="second")], complete=True)

    assert plan.status is PlanStatus.SUCCEEDED


def test_twin_pass_fingerprint_detects_nonvolatile_change():
    plan = PartitionPlan.create(
        instant(0),
        instant(1),
        ceiling=2,
        minimum_width=HOUR,
        volatile_metadata_paths=("/meta/lastUpdated", "/meta/versionId"),
    )
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("changed", active=True)], complete=True)

    plan.record_pass("root", 2, [resource("changed", active=False)], complete=True)

    assert plan.failure.code == "twin_pass_mismatch"


def test_twin_pass_detects_resource_id_set_change_even_when_counts_match():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("first")], complete=True)

    plan.record_pass("root", 2, [resource("second")], complete=True)

    assert plan.failure.code == "twin_pass_mismatch"


def test_fingerprint_is_key_order_independent_and_supports_escaped_pointer():
    first_resource_dict = {"id": "one", "meta": {"a/b": "volatile"}, "active": True}
    second_resource_dict = {"active": True, "meta": {"a/b": "changed"}, "id": "one"}

    assert fingerprint_resource(first_resource_dict, ["/meta/a~1b"]) == fingerprint_resource(
        second_resource_dict,
        ["/meta/a~1b"],
    )


def test_invalid_resource_id_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(1))

    plan.record_pass("root", 1, [{"resourceType": "Practitioner"}], complete=True)

    assert plan.failure.code == "invalid_resource_id"


def test_second_pass_cannot_precede_first_pass():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(0))

    with pytest.raises(PartitionPlanError, match="pass 1"):
        plan.record_pass("root", 2, [], complete=True)


def test_terminal_plan_rejects_additional_operations():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.unknown())

    with pytest.raises(PartitionPlanError, match="terminally failed"):
        plan.observe_count("root", CountObservation.exact(0))
