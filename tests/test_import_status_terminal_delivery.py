"""Regression coverage for terminal import status delivery."""

import asyncio
import threading
import time

import pytest

from process import import_status_events as status_events


@pytest.fixture(autouse=True)
def _reset_status_publisher():
    state = status_events._publisher_state
    state.queue = None
    state.worker = None
    state.loop = None
    state.pending.clear()
    state.coalesced_by_run.clear()
    state.flush_handle_by_run.clear()
    state.pending_terminal_events.clear()
    state.terminal_event_by_run.clear()
    state.terminal_delivery_by_run.clear()
    status_events._last_sent_by_run.clear()
    yield
    if isinstance(state.worker, asyncio.Task):
        state.worker.cancel()
    state.queue = None
    state.worker = None
    state.loop = None
    state.pending.clear()
    state.coalesced_by_run.clear()
    state.flush_handle_by_run.clear()
    state.pending_terminal_events.clear()
    state.terminal_event_by_run.clear()
    state.terminal_delivery_by_run.clear()
    status_events._last_sent_by_run.clear()


@pytest.mark.asyncio
async def test_terminal_status_event_bypasses_queued_progress(monkeypatch):
    delivered_statuses: list[str] = []
    first_post_started = threading.Event()
    release_first_post = threading.Event()

    def post_event(event):
        status = str(event["status"])
        if status == "running":
            if not first_post_started.is_set():
                first_post_started.set()
                assert release_first_post.wait(timeout=1)
            else:
                time.sleep(0.1)
        delivered_statuses.append(status)

    monkeypatch.setattr(status_events, "_status_event_url", lambda: "https://sink.invalid/events")
    monkeypatch.setattr(status_events, "_post_event", post_event)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 0.0)
    monkeypatch.setattr(status_events, "_timeout_seconds", lambda: 0.1)
    status_events.bind_status_event_loop()

    status_events.enqueue_status_event(
        {"run_id": "run-terminal", "status": "running", "phase_detail": "first"}
    )
    assert await asyncio.to_thread(first_post_started.wait, 1)
    for index in range(5):
        status_events.enqueue_status_event(
            {"run_id": "run-terminal", "status": "running", "phase_detail": f"queued-{index}"}
        )
        status_events.enqueue_status_event(
            {"run_id": f"run-unrelated-{index}", "status": "running", "phase_detail": "queued"}
        )
    status_events.enqueue_status_event({"run_id": "run-terminal", "status": "succeeded"})

    release_first_post.set()
    await status_events.flush_terminal_status_event("run-terminal", timeout_seconds=0.25)

    assert delivered_statuses == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_terminal_flush_binds_pending_event(monkeypatch):
    posted_events: list[dict[str, object]] = []
    monkeypatch.setattr(status_events, "_status_event_url", lambda: "https://sink.invalid/events")
    monkeypatch.setattr(status_events, "_post_event", posted_events.append)

    def enqueue_before_bind():
        status_events.enqueue_status_event(
            {"run_id": "run-before-bind", "status": "succeeded"}
        )
        status_events.enqueue_status_event(
            {"run_id": "run-before-bind", "status": "running"}
        )

    thread = threading.Thread(target=enqueue_before_bind)
    thread.start()
    thread.join()

    await status_events.flush_terminal_status_event("run-before-bind")

    assert [(event["run_id"], event["status"]) for event in posted_events] == [
        ("run-before-bind", "succeeded")
    ]
