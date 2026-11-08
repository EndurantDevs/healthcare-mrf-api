# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import MRF, MRF_start, mrf_initial_control


@pytest.fixture
def control_calls(monkeypatch):
    calls = SimpleNamespace(
        initialize=AsyncMock(),
        mark=AsyncMock(),
        flush=AsyncMock(),
        bind=Mock(),
    )
    monkeypatch.setattr(mrf_initial_control.initial, "init_file", calls.initialize)
    monkeypatch.setattr(mrf_initial_control, "mark_control_run", calls.mark)
    monkeypatch.setattr(mrf_initial_control, "_flush_terminal_status_events", calls.flush)
    monkeypatch.setattr(mrf_initial_control, "bind_status_event_loop", calls.bind)
    return calls


def test_mrf_workers_register_controlled_init_file():
    for settings in (MRF, MRF_start):
        initializers = [function for function in settings.functions if function.__name__ == "init_file"]
        assert initializers == [mrf_initial_control.init_file]


@pytest.mark.asyncio
async def test_success_binds_events_before_initialization_and_preserves_fanout(control_calls):
    context_by_field = {}
    task_by_field = {"run_id": "run-example"}
    result = object()

    async def initialize(actual_context, actual_task):
        control_calls.bind.assert_called_once_with()
        assert actual_context is context_by_field
        assert actual_task is task_by_field
        return result

    control_calls.initialize.side_effect = initialize

    assert await mrf_initial_control.init_file(context_by_field, task_by_field) is result
    control_calls.mark.assert_not_awaited()
    control_calls.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_initialization_failure_reports_and_flushes_before_reraising(control_calls):
    error = ValueError("invalid catalog row")
    control_calls.initialize.side_effect = error

    with pytest.raises(ValueError) as raised:
        await mrf_initial_control.init_file({}, {"run_id": "run-example"})

    assert raised.value is error
    control_calls.mark.assert_awaited_once_with(
        "run-example",
        status="failed",
        phase_detail="mrf initialization failed",
        progress_message="initialization failed",
        error={"code": "import_failed", "message": "invalid catalog row"},
    )
    control_calls.flush.assert_awaited_once_with("run-example")


@pytest.mark.parametrize("failing_report", ["mark", "flush"])
@pytest.mark.asyncio
async def test_reporting_failure_preserves_initialization_error(control_calls, failing_report, caplog):
    error = ValueError("invalid catalog row")
    control_calls.initialize.side_effect = error
    getattr(control_calls, failing_report).side_effect = RuntimeError("status unavailable")

    with pytest.raises(ValueError) as raised:
        await mrf_initial_control.init_file({}, {"run_id": "run-example"})

    assert raised.value is error
    assert "Failed to report MRF initialization failure" in caplog.text


@pytest.mark.parametrize("task_by_field", [None, {}, {"run_id": "  "}])
@pytest.mark.asyncio
async def test_initialization_without_control_run_does_not_report_failure(control_calls, task_by_field):
    error = ValueError("invalid catalog row")
    control_calls.initialize.side_effect = error

    with pytest.raises(ValueError) as raised:
        await mrf_initial_control.init_file({}, task_by_field)

    assert raised.value is error
    control_calls.mark.assert_not_awaited()
    control_calls.flush.assert_not_awaited()
