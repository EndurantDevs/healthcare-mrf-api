# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewer-facing margin for frozen control-event projection."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle
from tests.ptg_frozen_test_support import protected_control_payload


@pytest.mark.asyncio
async def test_missing_target_without_run_id_has_no_progress_token(monkeypatch):
    """Fail a targetless anonymous job without attempting token cleanup."""

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_flush_terminal_status_events",
        AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="target_module"):
        await control_lifecycle.control_single_job_start({}, {})


@pytest.mark.asyncio
async def test_rejected_attempt_resets_its_progress_token(monkeypatch):
    """Release the wrapper token when a newer attempt already owns the run."""

    reset_token_list = []
    monkeypatch.setattr(
        control_lifecycle,
        "set_live_progress_context",
        lambda **_payload: "live-token",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "reset_live_progress_context",
        reset_token_list.append,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        AsyncMock(return_value=False),
    )
    outcome_by_field = await control_lifecycle.control_single_job_start(
        {},
        {
            "run_id": "run-rejected",
            "target_module": "unused.module",
            "target_function": "unused",
        },
    )

    assert outcome_by_field["reason"] == "newer_attempt_active"
    assert reset_token_list == ["live-token"]


def test_incomplete_progress_context_does_not_claim_attempt(monkeypatch):
    """Reject an inherited attempt whose start time is absent."""

    monkeypatch.setattr(
        control_lifecycle,
        "current_live_progress_context",
        lambda: {"run_id": "run-one", "attempt_id": "attempt-one"},
    )
    assert control_lifecycle._control_attempt_for_run(
        "run-one",
        attempt_id=None,
        attempt_started_at=None,
    ) == (None, None)


@pytest.mark.asyncio
async def test_heartbeat_persistence_rejects_stale_attempt_progress(monkeypatch):
    """Discard stale live progress before persisting the current heartbeat."""

    monkeypatch.setattr(
        control_lifecycle,
        "read_live_progress",
        lambda _run_id: {
            "attempt_id": "old-attempt",
            "attempt_started_at": "2026-07-27T00:00:00+00:00",
        },
    )
    update_executor = AsyncMock(return_value=1)
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        update_executor,
    )

    is_persisted = await control_lifecycle._is_control_run_heartbeat_persisted(
        "run-heartbeat",
        "ptg_control_start",
        attempt_id="new-attempt",
        attempt_started_at="2026-07-28T00:00:00+00:00",
    )
    assert is_persisted is True
    update_executor.assert_awaited_once()


@pytest.mark.asyncio
async def test_terminal_mark_projects_frozen_metrics_and_snapshot(monkeypatch):
    """Write an opaque terminal event while retaining its public snapshot."""

    written_event_list = []
    monkeypatch.setattr(
        control_lifecycle,
        "_should_update_control_run_db",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **event_by_field: written_event_list.append(event_by_field),
    )
    private_params_by_name = protected_control_payload()["params"]

    is_marked = await control_lifecycle.mark_control_run(
        "run-frozen",
        status="succeeded",
        phase_detail="frozen import succeeded",
        progress_message="succeeded",
        metrics=private_params_by_name,
        snapshot_id="snapshot-frozen",
    )

    assert is_marked is True
    status_event = written_event_list[0]["status_event_payload"]
    assert status_event["snapshot_id"] == "snapshot-frozen"
    assert status_event["metrics"]["frozen_rate_file_set_protected"] is True
    assert status_event["metrics"]["frozen_rate_file_count"] == 2
    assert "frozen_rate_files" not in status_event["metrics"]
    assert "frozen_rate_file_set_sha256" not in status_event["metrics"]
