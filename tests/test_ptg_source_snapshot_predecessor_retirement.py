# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts import source_snapshot_predecessor_retirement_state as state
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
    PredecessorRetirementContext,
)


SOURCE_KEY = "synthetic-source"
CURRENT_SNAPSHOT_ID = "snapshot-current"
PREDECESSOR_SNAPSHOT_ID = "snapshot-previous"
ROLLBACK_OWNER_ID = "rollback-owner"
ACTOR = "operator@example.invalid"
REASON = "retention window complete"
IDEMPOTENCY_KEY = "retire-synthetic-001"


def _snapshot(snapshot_id: str, previous_snapshot_id: str | None) -> dict:
    return {
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "status": "published",
        "manifest": {
            "serving_index": {
                "source_key": SOURCE_KEY,
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
                "shared_snapshot_key": 17,
            }
        },
    }


def _source_pointer(**overrides) -> dict:
    pointer_by_field = {
        "source_key": SOURCE_KEY,
        "snapshot_id": CURRENT_SNAPSHOT_ID,
        "previous_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
    }
    pointer_by_field.update(overrides)
    return pointer_by_field


def _plan_pointer(**overrides) -> dict:
    pointer_by_field = {
        "plan_source_key": "synthetic-plan-key",
        "source_key": SOURCE_KEY,
        "snapshot_id": CURRENT_SNAPSHOT_ID,
        "previous_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
    }
    pointer_by_field.update(overrides)
    return pointer_by_field


def _pin(**overrides) -> dict:
    pin_by_field = {
        "owner_type": "ptg_v4_rollback",
        "owner_id": ROLLBACK_OWNER_ID,
        "snapshot_id": PREDECESSOR_SNAPSHOT_ID,
        "reason": "exact rollback retention",
    }
    pin_by_field.update(overrides)
    return pin_by_field


def _context(**overrides) -> PredecessorRetirementContext:
    context_by_field = {
        "snapshot_records": (
            _snapshot(CURRENT_SNAPSHOT_ID, PREDECESSOR_SNAPSHOT_ID),
            _snapshot(PREDECESSOR_SNAPSHOT_ID, None),
        ),
        "source_pointer_records": (_source_pointer(),),
        "plan_pointer_records": (_plan_pointer(),),
        "global_pointer_records": (
            {
                "slot": "current",
                "snapshot_id": CURRENT_SNAPSHOT_ID,
                "previous_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
            },
        ),
        "pin_records": (_pin(),),
        "control_pin_records": (),
        "release_binding_records": (),
        "control_release_binding_records": (),
    }
    context_by_field.update(overrides)
    return PredecessorRetirementContext(**context_by_field)


def _coordinates() -> dict[str, str]:
    return {
        "source_key": SOURCE_KEY,
        "current_snapshot_id": CURRENT_SNAPSHOT_ID,
        "predecessor_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
        "rollback_pin_mode": "owned",
        "rollback_owner_id": ROLLBACK_OWNER_ID,
    }


def test_retirement_decision_accepts_only_the_exact_live_predecessor_surface():
    decision = state.predecessor_retirement_decision(
        _context(),
        **_coordinates(),
    )

    assert decision.source_pointer_count == 1
    assert decision.plan_pointer_count == 1
    assert decision.global_pointer_count == 1


def test_absent_pin_mode_requires_zero_pins_and_deletes_none():
    coordinates_by_field = {
        **_coordinates(),
        "rollback_pin_mode": "absent",
        "rollback_owner_id": None,
    }
    decision = state.predecessor_retirement_decision(
        _context(pin_records=()),
        **coordinates_by_field,
    )

    assert decision.deleted_rollback_pin_count == 0
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="non-target retention pin",
    ):
        state.predecessor_retirement_decision(
            _context(),
            **coordinates_by_field,
        )
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="non-target retention pin",
    ):
        state.predecessor_retirement_decision(
            _context(
                pin_records=(),
                control_pin_records=(
                    {
                        "owner_type": "plan_release",
                        "owner_id": "release-1",
                        "snapshot_id": PREDECESSOR_SNAPSHOT_ID,
                    },
                ),
            ),
            **coordinates_by_field,
        )


def test_pin_mode_and_owner_are_an_explicit_request_pair():
    request_by_field = {
        **_coordinates(),
        "actor": ACTOR,
        "reason": REASON,
        "idempotency_key": IDEMPOTENCY_KEY,
    }
    with pytest.raises(ValueError, match="rollback_owner_id must be omitted"):
        retirement.normalized_predecessor_retirement_request(
            **{
                **request_by_field,
                "rollback_pin_mode": "absent",
            },
        )
    with pytest.raises(ValueError, match="rollback_owner_id is required"):
        retirement.normalized_predecessor_retirement_request(
            **{
                **request_by_field,
                "rollback_pin_mode": "owned",
                "rollback_owner_id": None,
            },
        )


@pytest.mark.parametrize(
    ("context", "message"),
    [
        (
            _context(
                snapshot_records=(
                    _snapshot(CURRENT_SNAPSHOT_ID, "different-predecessor"),
                    _snapshot(PREDECESSOR_SNAPSHOT_ID, None),
                )
            ),
            "snapshot lineage",
        ),
        (
            _context(
                source_pointer_records=(
                    _source_pointer(previous_snapshot_id=None),
                )
            ),
            "source pointer",
        ),
        (
            _context(
                plan_pointer_records=(
                    _plan_pointer(snapshot_id="different-current"),
                )
            ),
            "plan pointer",
        ),
        (
            _context(
                global_pointer_records=(
                    {
                        "slot": "current",
                        "snapshot_id": CURRENT_SNAPSHOT_ID,
                        "previous_snapshot_id": None,
                    },
                )
            ),
            "global pointer",
        ),
        (
            _context(
                source_pointer_records=(
                    _source_pointer(),
                    _source_pointer(
                        source_key="another-source",
                    ),
                )
            ),
            "unexpected live reference",
        ),
        (
            _context(
                pin_records=(
                    _pin(),
                    _pin(owner_type="plan_release", owner_id="release-1"),
                )
            ),
            "non-target retention pin",
        ),
        (
            _context(pin_records=(_pin(owner_id="wrong-owner"),)),
            "exact rollback pin",
        ),
        (
            _context(
                control_pin_records=(
                    {
                        "owner_type": "plan_release",
                        "owner_id": "release-1",
                        "snapshot_id": PREDECESSOR_SNAPSHOT_ID,
                    },
                )
            ),
            "non-target retention pin",
        ),
        (
            _context(
                release_binding_records=(
                    {
                        "serving_revision_id": "release-1",
                        "role": "in_network",
                        "binding_ordinal": 0,
                    },
                )
            ),
            "release binding",
        ),
        (
            _context(
                control_release_binding_records=(
                    {
                        "release_binding_id": "control-binding-1",
                        "serving_revision_id": "release-1",
                        "role": "in_network",
                        "ordinal": 0,
                    },
                )
            ),
            "release binding",
        ),
    ],
)
def test_retirement_decision_rejects_drift_and_nonexclusive_retention(
    context,
    message,
):
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match=message,
    ):
        state.predecessor_retirement_decision(context, **_coordinates())


def test_retirement_ignores_an_unrelated_global_pointer():
    decision = state.predecessor_retirement_decision(
        _context(
            global_pointer_records=(
                {
                    "slot": "current",
                    "snapshot_id": "another-current",
                    "previous_snapshot_id": "another-previous",
                },
            )
        ),
        **_coordinates(),
    )

    assert decision.global_pointer_count == 0


def test_retirement_rejects_an_extra_current_reference_to_predecessor():
    context = replace(
        _context(),
        plan_pointer_records=(
            _plan_pointer(),
            _plan_pointer(
                plan_source_key="stale-plan-key",
                snapshot_id=PREDECESSOR_SNAPSHOT_ID,
                previous_snapshot_id=None,
            ),
        ),
    )

    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="unexpected live reference",
    ):
        state.predecessor_retirement_decision(context, **_coordinates())


def test_retirement_rejects_non_source_plan_and_orphan_global_references():
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="unexpected live reference",
    ):
        state.predecessor_retirement_decision(
            _context(
                plan_pointer_records=(
                    _plan_pointer(),
                    _plan_pointer(
                        plan_source_key="other-plan",
                        source_key="other-source",
                        snapshot_id="other-current",
                        previous_snapshot_id=PREDECESSOR_SNAPSHOT_ID,
                    ),
                )
            ),
            **_coordinates(),
        )
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="unexpected live reference",
    ):
        state.predecessor_retirement_decision(
            _context(
                global_pointer_records=(
                    {
                        "slot": "current",
                        "snapshot_id": "other-current",
                        "previous_snapshot_id": PREDECESSOR_SNAPSHOT_ID,
                    },
                )
            ),
            **_coordinates(),
        )


@pytest.mark.parametrize("field", ["actor", "reason", "idempotency_key"])
def test_retirement_requires_nonempty_audit_identity_fields(field):
    request_by_field = {
        **_coordinates(),
        "actor": ACTOR,
        "reason": REASON,
        "idempotency_key": IDEMPOTENCY_KEY,
    }
    request_by_field[field] = " "

    with pytest.raises(ValueError, match="are required"):
        retirement.normalized_predecessor_retirement_request(
            **request_by_field
        )


def test_retirement_rejects_oversized_and_same_snapshot_coordinates():
    request_by_field = {
        **_coordinates(),
        "actor": ACTOR,
        "reason": REASON,
        "idempotency_key": "x" * 161,
    }
    with pytest.raises(ValueError, match="idempotency_key"):
        retirement.normalized_predecessor_retirement_request(
            **request_by_field
        )
    request_by_field["idempotency_key"] = IDEMPOTENCY_KEY
    request_by_field["predecessor_snapshot_id"] = CURRENT_SNAPSHOT_ID
    with pytest.raises(ValueError, match="must differ"):
        retirement.normalized_predecessor_retirement_request(
            **request_by_field
        )


@pytest.mark.parametrize("manifest", [None, "{", "[]", {"serving_index": "x"}])
def test_retirement_fails_closed_on_invalid_snapshot_manifest(manifest):
    context = _context(
        snapshot_records=(
            {
                **_snapshot(
                    CURRENT_SNAPSHOT_ID,
                    PREDECESSOR_SNAPSHOT_ID,
                ),
                "manifest": manifest,
            },
            _snapshot(PREDECESSOR_SNAPSHOT_ID, None),
        )
    )

    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="snapshot lineage",
    ):
        state.predecessor_retirement_decision(context, **_coordinates())
