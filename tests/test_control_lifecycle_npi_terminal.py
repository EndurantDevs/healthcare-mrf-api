# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal control projections for an atomically published NPI run."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle


@pytest.mark.asyncio
async def test_committed_control_run_emits_authoritative_database_times(monkeypatch):
    live_events = []
    database_update = AsyncMock(
        side_effect=AssertionError("committed state must not be rewritten")
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        database_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **payload: live_events.append(payload),
    )
    committed_at = "2026-08-09T02:03:04.567890+00:00"

    is_accepted = await control_lifecycle.mark_control_run(
        "run_committed",
        status="succeeded",
        phase_detail="npi published",
        progress_message="succeeded",
        snapshot_id="nppub1_" + "a" * 43,
        database_state_committed=True,
        database_heartbeat_at=committed_at,
        database_finished_at=committed_at,
    )

    assert is_accepted is True
    database_update.assert_not_awaited()
    assert live_events[-1]["finished_at"] == committed_at
    status_event_by_name = live_events[-1]["status_event_payload"]
    assert status_event_by_name["heartbeat_at"] == committed_at
    assert status_event_by_name["finished_at"] == committed_at
    assert status_event_by_name["snapshot_id"] == "nppub1_" + "a" * 43


@pytest.mark.asyncio
async def test_committed_control_run_requires_exact_database_times():
    with pytest.raises(ValueError, match="committed control timestamp"):
        await control_lifecycle.mark_control_run(
            "run_committed",
            status="succeeded",
            phase_detail="npi published",
            progress_message="succeeded",
            database_state_committed=True,
        )


def test_terminal_progress_envelope_is_not_copied_into_metrics():
    result_by_name = {
        "rows": 4,
        "terminal_progress": {
            "unit": "rows",
            "done": 4,
            "total": 4,
            "pct": 100,
            "message": "succeeded",
            "phase": "npi published",
        },
    }

    assert control_lifecycle._terminal_metrics_from_result(result_by_name) == {
        "rows": 4
    }
    assert control_lifecycle._terminal_progress_from_result(
        "process_data",
        result_by_name,
    ) == result_by_name["terminal_progress"]


@pytest.mark.asyncio
async def test_committed_terminal_projection_drains_worker_cancellation():
    parent_task = asyncio.current_task()
    completed_steps = []

    async def terminal_projection():
        assert parent_task is not None
        parent_task.cancel()
        await asyncio.sleep(0)
        completed_steps.append(True)

    await control_lifecycle._drain_committed_terminal_projection(
        terminal_projection()
    )

    assert completed_steps == [True]
    assert parent_task is not None
    assert parent_task.cancelling() == 0
