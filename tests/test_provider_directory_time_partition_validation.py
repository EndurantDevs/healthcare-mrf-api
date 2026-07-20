# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime

import pytest

from process.provider_directory_time_partition import (
    CountKind,
    CountObservation,
    PartitionConfig,
    PartitionPlan,
    PartitionPlanError,
    TimeWindow,
    WindowState,
)
from process.provider_directory_time_partition_primitives import (
    decode_json_pointer,
    fingerprint_resource,
    validate_utc_instant,
)


UTC = datetime.UTC
HOUR = datetime.timedelta(hours=1)


def _instant(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 1, 1, hour, tzinfo=UTC)


def _resource(resource_id: str, *, active: bool = True) -> dict[str, object]:
    return {"resourceType": "Practitioner", "id": resource_id, "active": active}


def _new_plan() -> PartitionPlan:
    return PartitionPlan.create(
        _instant(0),
        _instant(2),
        ceiling=2,
        minimum_width=HOUR,
    )


def _new_checkpoint() -> dict[str, object]:
    return _new_plan().to_dict()


def _split_checkpoint() -> dict[str, object]:
    plan = _new_plan()
    plan.observe_count("root", CountObservation.exact(3))
    plan.observe_count("root.0", CountObservation.exact(1))
    plan.observe_count("root.1", CountObservation.exact(1))
    return plan.to_dict()


@pytest.mark.parametrize(
    ("observation_fields_by_name", "message"),
    [
        ({"kind": "exact", "count": 1}, "kind is invalid"),
        (
            {"kind": CountKind.EXACT, "count": 1, "detail": "unexpected"},
            "cannot include an error detail",
        ),
        (
            {"kind": CountKind.ERROR, "count": 1},
            "cannot include a count",
        ),
    ],
)
def test_count_observation_rejects_inconsistent_fields(
    observation_fields_by_name,
    message,
):
    with pytest.raises(PartitionPlanError, match=message):
        CountObservation(**observation_fields_by_name)


def test_time_window_rejects_empty_interval():
    with pytest.raises(PartitionPlanError, match="start < end"):
        TimeWindow("empty", _instant(1), _instant(1))


@pytest.mark.parametrize(
    ("config_fields_by_name", "message"),
    [
        (
            {"ceiling": True, "minimum_width": HOUR},
            "ceiling must be a non-negative integer",
        ),
        (
            {"ceiling": 1, "minimum_width": 1},
            "minimum_width must be a timedelta",
        ),
        (
            {"ceiling": 1, "minimum_width": datetime.timedelta(0)},
            "minimum_width must be positive",
        ),
        (
            {
                "ceiling": 1,
                "minimum_width": HOUR,
                "boundary_precision": 1,
            },
            "boundary_precision must be a timedelta",
        ),
        (
            {
                "ceiling": 1,
                "minimum_width": HOUR,
                "boundary_precision": datetime.timedelta(0),
            },
            "boundary_precision must be positive",
        ),
    ],
)
def test_partition_config_rejects_invalid_scalar_contracts(
    config_fields_by_name,
    message,
):
    with pytest.raises(PartitionPlanError, match=message):
        PartitionConfig(**config_fields_by_name)


def test_split_window_rejects_a_new_count():
    plan = _new_plan()
    plan.observe_count("root", CountObservation.exact(3))
    plan.windows["root"].count = None

    with pytest.raises(PartitionPlanError, match="does not accept a count"):
        plan.observe_count("root", CountObservation.exact(1))


def test_record_pass_rejects_unready_window_and_invalid_pass_number():
    plan = _new_plan()
    with pytest.raises(PartitionPlanError, match="not ready"):
        plan.record_pass("root", 1, [], complete=True)

    plan.observe_count("root", CountObservation.exact(0))
    with pytest.raises(PartitionPlanError, match="must be 1 or 2"):
        plan.record_pass("root", 3, [], complete=True)


def test_pass_replay_is_idempotent_then_rejects_changed_content():
    plan = _new_plan()
    plan.observe_count("root", CountObservation.exact(1))
    first_resources = [_resource("one")]
    plan.record_pass("root", 1, first_resources, complete=True)
    plan.record_pass("root", 1, first_resources, complete=True)

    assert plan.failure is None

    plan.record_pass("root", 1, [_resource("two")], complete=True)
    assert plan.failure is not None
    assert plan.failure.code == "contradictory_pass"


def test_record_pass_rejects_non_mapping_resource():
    plan = _new_plan()
    plan.observe_count("root", CountObservation.exact(1))

    plan.record_pass("root", 1, [object()], complete=True)

    assert plan.failure is not None
    assert plan.failure.code == "invalid_resource"


def test_succeeded_plan_and_unknown_window_reject_new_operations():
    plan = _new_plan()
    with pytest.raises(PartitionPlanError, match="unknown window"):
        plan.observe_count("missing", CountObservation.exact(0))

    plan.observe_count("root", CountObservation.exact(0))
    plan.record_pass("root", 1, [], complete=True)
    plan.record_pass("root", 2, [], complete=True)
    with pytest.raises(PartitionPlanError, match="already succeeded"):
        plan.observe_count("root", CountObservation.exact(0))


def _remove_root(checkpoint_by_field):
    checkpoint_by_field["windows"][0]["window_id"] = "other"


def _move_first_leaf_inside_root(checkpoint_by_field):
    checkpoint_by_field["windows"][1]["start"] = (
        "2026-01-01T00:00:01.000000Z"
    )


def _add_pass_to_uncounted_window(checkpoint_by_field):
    checkpoint_by_field["windows"][0]["passes"] = {"1": {}}


def _set_count_on_uncounted_window(checkpoint_by_field):
    checkpoint_by_field["windows"][0]["count"] = 0


def _make_ready_window_without_count(checkpoint_by_field):
    checkpoint_by_field["windows"][0]["state"] = "ready"


def _make_ready_window_exceed_ceiling(checkpoint_by_field):
    checkpoint_by_field["windows"][0].update({"state": "ready", "count": 3})


def _make_pass_count_mismatch(checkpoint_by_field):
    checkpoint_by_field["windows"][0].update(
        {"state": "ready", "count": 1, "passes": {"1": {}}}
    )


def _make_twin_pass_mismatch(checkpoint_by_field):
    checkpoint_by_field["windows"][0].update(
        {
            "state": "ready",
            "count": 1,
            "passes": {"1": {"one": "first"}, "2": {"one": "second"}},
        }
    )


def _repeat_resource_across_windows(checkpoint_by_field):
    for window_by_field in checkpoint_by_field["windows"][1:]:
        window_by_field["passes"] = {"1": {"same": "fingerprint"}}


@pytest.mark.parametrize(
    ("checkpoint_factory", "mutate_checkpoint", "message"),
    [
        (_new_checkpoint, _remove_root, "missing the root"),
        (_split_checkpoint, _move_first_leaf_inside_root, "cover the root"),
        (
            _new_checkpoint,
            _add_pass_to_uncounted_window,
            "only ready checkpoint windows",
        ),
        (
            _new_checkpoint,
            _set_count_on_uncounted_window,
            "uncounted checkpoint window has a count",
        ),
        (
            _new_checkpoint,
            _make_ready_window_without_count,
            "ready checkpoint window is missing its count",
        ),
        (
            _new_checkpoint,
            _make_ready_window_exceed_ceiling,
            "ready checkpoint window exceeds the ceiling",
        ),
        (
            _new_checkpoint,
            _make_pass_count_mismatch,
            "checkpoint pass does not reconcile",
        ),
        (
            _new_checkpoint,
            _make_twin_pass_mismatch,
            "checkpoint twin passes differ",
        ),
        (
            _split_checkpoint,
            _repeat_resource_across_windows,
            "repeats a resource ID across windows",
        ),
    ],
)
def test_checkpoint_rejects_inconsistent_state(
    checkpoint_factory,
    mutate_checkpoint,
    message,
):
    checkpoint_by_field = checkpoint_factory()
    mutate_checkpoint(checkpoint_by_field)

    with pytest.raises(PartitionPlanError, match=message):
        PartitionPlan.from_dict(checkpoint_by_field)


def test_checkpoint_rejects_duplicate_windows_and_missing_config():
    duplicate_checkpoint_by_field = _new_checkpoint()
    duplicate_checkpoint_by_field["windows"].append(
        dict(duplicate_checkpoint_by_field["windows"][0])
    )
    with pytest.raises(PartitionPlanError, match="invalid partition checkpoint"):
        PartitionPlan.from_dict(duplicate_checkpoint_by_field)

    incomplete_checkpoint_by_field = _new_checkpoint()
    del incomplete_checkpoint_by_field["config"]["boundary_precision_microseconds"]
    with pytest.raises(PartitionPlanError, match="invalid partition checkpoint"):
        PartitionPlan.from_dict(incomplete_checkpoint_by_field)


def test_checkpoint_rejects_version_json_and_non_object_payloads():
    checkpoint_by_field = _new_checkpoint()
    checkpoint_by_field["version"] = 1
    with pytest.raises(PartitionPlanError, match="unsupported"):
        PartitionPlan.from_dict(checkpoint_by_field)
    with pytest.raises(PartitionPlanError, match="invalid partition checkpoint JSON"):
        PartitionPlan.from_json("{")
    with pytest.raises(PartitionPlanError, match="must be an object"):
        PartitionPlan.from_json("[]")


def test_partition_primitives_reject_invalid_inputs_and_remove_list_paths():
    with pytest.raises(PartitionPlanError, match="must be a datetime"):
        validate_utc_instant("not-a-datetime")
    with pytest.raises(PartitionPlanError, match="non-root JSON Pointers"):
        decode_json_pointer("invalid")

    first_nested_by_field = {
        "id": "one",
        "items": [{"secret": "first", "keep": 1}],
    }
    second_nested_by_field = {
        "id": "one",
        "items": [{"secret": "second", "keep": 1}],
    }
    assert fingerprint_resource(first_nested_by_field, ["/items/0/secret"]) == (
        fingerprint_resource(second_nested_by_field, ["/items/0/secret"])
    )
    assert fingerprint_resource(
        {"id": "one", "items": ["stable", "first"]},
        ["/items/1"],
    ) == fingerprint_resource(
        {"id": "one", "items": ["stable", "second"]},
        ["/items/1"],
    )
    fingerprint_resource(first_nested_by_field, ["/items/9/secret"])
    fingerprint_resource(first_nested_by_field, ["/items/not-an-index"])
