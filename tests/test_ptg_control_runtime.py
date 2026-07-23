# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process import ptg_control, ptg_control_runtime


_WAIT_CONTINUES = False
_WAIT_STOPS = True


class ImmediateHeartbeatEvent:
    def __init__(self):
        self.wait_calls = 0
        self.has_been_set = False

    def wait(self, _interval):
        self.wait_calls += 1
        return _WAIT_STOPS if self.wait_calls > 1 else _WAIT_CONTINUES

    def set(self):
        self.has_been_set = True


class ImmediateHeartbeatThread:
    def __init__(self, target, **_arguments_by_name):
        self.target = target

    def start(self):
        self.target()


def test_ptg_thread_heartbeat_preserves_importer_progress_source(monkeypatch):
    progress_events = []
    monkeypatch.setattr(
        ptg_control_runtime.threading,
        "Event",
        ImmediateHeartbeatEvent,
    )
    monkeypatch.setattr(
        ptg_control_runtime.threading,
        "Thread",
        ImmediateHeartbeatThread,
    )
    monkeypatch.setattr(
        ptg_control_runtime,
        "write_live_progress",
        lambda **progress_by_field: progress_events.append(progress_by_field),
    )

    stop_event = ptg_control._start_threaded_ptg_heartbeat(
        "run_ptg",
        "2026-07-03T12:00:00+00:00",
    )
    ptg_control._stop_threaded_ptg_heartbeat(stop_event)

    assert progress_events
    assert progress_events[0]["source"] == ptg_control.PTG_CONTROL_HEARTBEAT_SOURCE
    assert progress_events[0]["confidence"] == "heartbeat"
    assert progress_events[0]["publish_event"] is False
    assert stop_event.has_been_set


def test_ptg_heartbeat_can_be_disabled(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")

    stop_event = ptg_control._start_threaded_ptg_heartbeat("run", "started")

    assert not stop_event.is_set()


@pytest.mark.asyncio
async def test_ptg_control_start_skips_stale_terminal_run(monkeypatch):
    async def fail_if_scanner_starts(**_arguments_by_name):
        raise AssertionError("stale PTG job should not start scanner")

    async def fail_if_run_is_marked(*_arguments, **_arguments_by_name):
        raise AssertionError("stale PTG job should not mark run running")

    async def canceled_database_row(*_arguments, **_arguments_by_name):
        return ("canceled",)

    monkeypatch.setattr(ptg_control, "ptg_main", fail_if_scanner_starts)
    monkeypatch.setattr(ptg_control, "mark_control_run", fail_if_run_is_marked)
    monkeypatch.setattr(
        ptg_control_runtime.db,
        "first",
        canceled_database_row,
    )

    control_result = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run_old",
            "source_file_import_id": "source_import_1",
            "params": {"test_mode": True, "source_key": "demo_source"},
        },
    )

    assert control_result == {
        "status": "skipped",
        "run_id": "run_old",
        "reason": "run_canceled",
    }


@pytest.mark.asyncio
async def test_ptg_stale_job_check_allows_nonterminal_local_run(monkeypatch):
    async def queued_database_row(*_arguments, **_arguments_by_name):
        return ("queued",)

    monkeypatch.setattr(
        ptg_control_runtime.db,
        "first",
        queued_database_row,
    )

    assert await ptg_control._stale_ptg_job_result("run_old") is None


@pytest.mark.asyncio
async def test_ptg_stale_and_flush_helpers_skip_absent_work(monkeypatch):
    assert await ptg_control._stale_ptg_job_result("") is None

    async def missing_database_row(*_arguments, **_arguments_by_name):
        return None

    monkeypatch.setattr(
        ptg_control_runtime.db,
        "first",
        missing_database_row,
    )
    assert await ptg_control._stale_ptg_job_result("run") is None

    monkeypatch.setenv(
        "HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS",
        "0",
    )
    await ptg_control._flush_terminal_status_events()
