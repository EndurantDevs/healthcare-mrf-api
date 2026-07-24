# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attempt-fence identity contracts for stale V4 metadata plans."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace

import pytest

from tests.test_ptg2_v4_stale_metadata_reconcile import (
    FENCE_NONCE,
    NOW,
    _plan,
    _ready_context,
)


def _context_with_fence_overrides(fence_overrides: dict):
    """Return a ready context with one explicit authority mutation."""

    context = _ready_context()
    return replace(
        context,
        attempt_fence_by_field={
            **dict(context.attempt_fence_by_field or {}),
            **fence_overrides,
        },
    )


@pytest.mark.parametrize(
    "fence_overrides",
    (
        {"target_digest": "1" * 64},
        {"plan_digest": "2" * 64},
        {"marker_digest": "3" * 64},
        {"marker": {"unexpected": True}, "marker_is_sql_null": False},
        {"marker": None, "marker_is_sql_null": False},
        {"reconciled_at": NOW},
        {"fence_nonce": None},
        {"created_at": None},
        {"snapshot_id": "ptg2:different"},
        {"internal_run_id": "ptg2:different-run"},
    ),
)
def test_active_fence_requires_a_pristine_audit_shape(fence_overrides):
    plan_by_field = _plan(_context_with_fence_overrides(fence_overrides))

    assert plan_by_field["status"] == "ineligible"
    assert (
        "attempt_fence_audit_not_pristine"
        in plan_by_field["reason_codes"]
    )


@pytest.mark.parametrize(
    "fence_overrides",
    (
        {"fence_nonce": "22222222-2222-4222-8222-222222222222"},
        {"created_at": NOW - dt.timedelta(hours=6)},
    ),
)
def test_fence_identity_changes_invalidate_the_reviewed_digest(
    fence_overrides,
):
    reviewed_plan = _plan(_ready_context())
    changed_plan = _plan(_context_with_fence_overrides(fence_overrides))

    assert reviewed_plan["status"] == changed_plan["status"] == "ready"
    assert reviewed_plan["plan_digest"] != changed_plan["plan_digest"]


def test_fence_identity_is_not_disclosed_by_the_plan():
    serialized_plan = json.dumps(_plan(_ready_context()), sort_keys=True)

    assert FENCE_NONCE not in serialized_plan
    assert "fence_nonce" not in serialized_plan


def test_missing_reviewed_fence_is_ineligible():
    context = _ready_context()
    missing_context = replace(
        context,
        attempt_fence_by_field=None,
    )

    plan_by_field = _plan(missing_context)

    assert plan_by_field["status"] == "ineligible"
    assert "attempt_fence_missing" in plan_by_field["reason_codes"]
