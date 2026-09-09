# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A committed Kentucky result survives later worker cancellation or errors."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle as lifecycle
from process.control_cancel import ImportCancelledError


def _committed_context():
    return {"control_run_terminal_committed": True, "preserve_control_run_finished_at": True,
            "_control_committed_heartbeat_at": "2026-09-08T08:01:00.000000+00:00",
            "_control_committed_finished_at": "2026-09-08T08:01:00.000000+00:00",
            "_control_committed_result": {"published": True, "requested_licenses": 1, "terminal_progress": {
                "phase": "kentucky-kbml-profile published", "unit": "license", "done": 1, "total": 1,
                "pct": 100, "message": "succeeded",
            }}}


@pytest.mark.parametrize("target_module", ["process.kentucky_profile", "process.unrelated"])
@pytest.mark.parametrize("late_error", [None, RuntimeError, asyncio.CancelledError, ImportCancelledError])
async def test_only_supported_target_keeps_committed_result_after_late_error(monkeypatch, target_module, late_error):
    """Project the committed receipt and times without rewriting durable success."""
    async def target(job_context, _task):
        job_context["context"].update(_committed_context())
        if late_error:
            raise late_error("after commit")
        return {"incorrect_later_result": True}

    projection = AsyncMock()
    monkeypatch.setattr(lifecycle, "import_module", lambda _name: SimpleNamespace(import_profiles=target))
    monkeypatch.setattr(lifecycle, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(lifecycle, "_mark_and_flush_terminal_control_run", projection)
    monkeypatch.setattr(lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(lifecycle, "_live_progress_heartbeat", AsyncMock())
    task_by_field = {"run_id": "run_ky_synthetic", "importer": "kentucky-kbml-profile", "target_module": target_module,
                     "target_function": "import_profiles", "run_shutdown": False, "task": {}}
    if target_module == "process.unrelated" and late_error is RuntimeError:
        with pytest.raises(RuntimeError, match="after commit"):
            await lifecycle.control_single_job_start({}, task_by_field)
        projection.assert_not_awaited()
        return
    result_by_field = await lifecycle.control_single_job_start({}, task_by_field)
    if target_module == "process.unrelated" and late_error:
        assert result_by_field["status"] in {"failed", "canceled"}
        projection.assert_not_awaited()
    else:
        assert result_by_field["status"] == "succeeded"
        projected = projection.await_args.kwargs
        assert projected["database_state_committed"] is (target_module == "process.kentucky_profile")
        if target_module == "process.kentucky_profile":
            assert result_by_field["result"] == _committed_context()["_control_committed_result"]
            assert projected["database_finished_at"] == _committed_context()["_control_committed_finished_at"]
            assert projected["phase_detail"] == "kentucky-kbml-profile published"


@pytest.mark.parametrize("context", [{}, {"control_run_terminal_committed": False},
                                  {"control_run_terminal_committed": True, "_control_committed_result": []}])
def test_kentucky_requires_an_explicit_committed_result(context):
    assert lifecycle._committed_target_result({"context": context}, target_module="process.kentucky_profile") is None
