# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Review authority at the stale-policy time boundary."""

from __future__ import annotations

import datetime as dt

import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from tests.test_ptg2_v4_stale_metadata_reconcile import (
    INTERNAL_RUN_ID,
    NOW,
    SNAPSHOT_ID,
    _plan,
    _ready_context,
)


def test_pre_threshold_digest_cannot_execute_after_cutoff() -> None:
    heartbeat_at = NOW - dt.timedelta(hours=5, minutes=59)
    before_context = _ready_context(
        internal_run_overrides={"heartbeat_at": heartbeat_at}
    )
    after_context = _ready_context(
        observed_at=NOW + dt.timedelta(minutes=2),
        internal_run_overrides={"heartbeat_at": heartbeat_at},
    )
    reviewed_plan = _plan(before_context)
    executable_plan = _plan(after_context)

    assert reviewed_plan["status"] == "ineligible"
    assert "internal_run_not_stale" in reviewed_plan["reason_codes"]
    assert executable_plan["status"] == "ready"
    assert reviewed_plan["plan_digest"] != executable_plan["plan_digest"]

    request = reconcile.PTG2V4StaleMetadataRequest(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        expected_plan_digest=reviewed_plan["plan_digest"],
        stale_after_seconds=21_600,
        target_digest=reconcile.stale_target_digest(
            SNAPSHOT_ID, INTERNAL_RUN_ID
        ),
        schema_name="mrf",
    )
    with pytest.raises(
        reconcile.PTG2V4StaleMetadataConflict,
        match="state changed after plan review",
    ):
        reconcile._reviewed_marker(after_context, request, executable_plan)


def test_fresh_ready_digest_executes_after_cutoff() -> None:
    context = _ready_context()
    ready_plan = _plan(context)
    request = reconcile.PTG2V4StaleMetadataRequest(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        expected_plan_digest=ready_plan["plan_digest"],
        stale_after_seconds=21_600,
        target_digest=reconcile.stale_target_digest(
            SNAPSHOT_ID, INTERNAL_RUN_ID
        ),
        schema_name="mrf",
    )

    marker_by_field, is_idempotent = reconcile._reviewed_marker(
        context, request, ready_plan
    )

    assert is_idempotent is False
    assert marker_by_field["plan_digest"] == ready_plan["plan_digest"]
