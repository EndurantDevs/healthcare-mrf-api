from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

florida = importlib.import_module("process.florida_mqa_profile")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "time_values",
    [
        [0.0, 0.0, 11.0],
        [0.0, 0.0, 0.0, 11.0],
    ],
)
async def test_failure_status_retry_stops_at_each_deadline_boundary(
    monkeypatch,
    time_values,
):
    class Loop:
        def __init__(self):
            self.values = iter(time_values)

        def time(self):
            return next(self.values)

    class Update:
        def where(self, *_criteria):
            return self

        def values(self, **_values):
            return self

        async def status(self):
            raise ConnectionError("database recovering")

    async def wait_for(awaitable, timeout):
        del timeout
        return await awaitable

    monkeypatch.setattr(florida, "_failure_status_attempts", lambda: 2)
    monkeypatch.setattr(florida, "_failure_status_timeout_seconds", lambda: 1)
    monkeypatch.setattr(florida, "_failure_status_window_seconds", lambda: 10)
    monkeypatch.setattr(florida.asyncio, "get_running_loop", lambda: Loop())
    monkeypatch.setattr(florida.asyncio, "wait_for", wait_for)
    monkeypatch.setattr(
        florida,
        "_dispose_failed_database_pool",
        AsyncMock(),
    )
    monkeypatch.setattr(florida.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(florida.db, "update", lambda _table: Update())

    operation_result = await florida._mark_failed_run_status(
        run_id="c" * 32,
        run_row={"metrics": {}},
        original_error=RuntimeError("import failed"),
        cleanup_error=None,
    )

    assert operation_result == "ConnectionError: database recovering"


@pytest.mark.asyncio
async def test_failure_status_zero_attempt_budget_reports_unknown(monkeypatch):
    monkeypatch.setattr(florida, "_failure_status_attempts", lambda: 0)
    monkeypatch.setattr(florida, "_failure_status_window_seconds", lambda: 10)

    result = await florida._mark_failed_run_status(
        run_id="b" * 32,
        run_row={"metrics": {}},
        original_error=RuntimeError("import failed"),
        cleanup_error=None,
    )

    assert result == "unknown failure while recording failed import status"
