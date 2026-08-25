"""Regression coverage for terminal import status delivery."""

import asyncio
import threading
import time
from contextlib import suppress

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


@pytest.mark.asyncio
async def test_pending_capacity_preserves_terminals_across_runs(monkeypatch):
    posted_events: list[dict[str, object]] = []
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "1")
    monkeypatch.setattr(status_events, "_status_event_url", lambda: "https://sink.invalid/events")
    monkeypatch.setattr(status_events, "_post_event", posted_events.append)

    def enqueue_before_bind():
        status_events.enqueue_status_event({"run_id": "run-a", "status": "succeeded"})
        status_events.enqueue_status_event({"run_id": "run-b", "status": "failed"})
        status_events.enqueue_status_event({"run_id": "run-c", "status": "running"})

    thread = threading.Thread(target=enqueue_before_bind)
    thread.start()
    thread.join()

    await status_events.flush_terminal_status_event("run-a")
    await status_events.flush_terminal_status_event("run-b")

    assert sorted((event["run_id"], event["status"]) for event in posted_events) == [
        ("run-a", "succeeded"),
        ("run-b", "failed"),
    ]


@pytest.mark.asyncio
async def test_repeated_terminal_suppresses_later_progress(monkeypatch):
    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: queue)

    first_terminal_by_field = {"run_id": "run-repeat", "status": "succeeded"}
    status_events._accept_event_on_loop(loop, first_terminal_by_field)
    first_delivery = status_events._publisher_state.terminal_delivery_by_run[
        "run-repeat"
    ]
    status_events._accept_event_on_loop(
        loop,
        {"run_id": "run-repeat", "status": "failed"},
    )
    assert await first_delivery is False

    queued_count = queue.qsize()
    status_events._accept_event_on_loop(
        loop,
        {"run_id": "run-repeat", "status": "running"},
    )
    assert queue.qsize() == queued_count


@pytest.mark.asyncio
async def test_terminal_flush_handles_disabled_and_missing_delivery(monkeypatch):
    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    monkeypatch.setattr(status_events, "_status_event_url", lambda: "")
    await status_events.flush_terminal_status_event("ignored")
    monkeypatch.setattr(status_events, "_status_event_url", lambda: "https://sink.invalid")
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: queue)

    await status_events.flush_terminal_status_event("disabled", timeout_seconds=0)
    await status_events.flush_terminal_status_event("missing", timeout_seconds=0.25)
    assert status_events._terminal_flush_timeout_seconds(0) == 0.0


@pytest.mark.asyncio
async def test_rebinding_loop_cancels_only_pending_terminal_deliveries():
    loop = asyncio.get_running_loop()
    pending_delivery = loop.create_future()
    completed_delivery = loop.create_future()
    completed_delivery.set_result(True)
    status_events._publisher_state.loop = object()
    status_events._publisher_state.terminal_delivery_by_run.update(
        {"pending": pending_delivery, "completed": completed_delivery}
    )

    status_events._ensure_queue(loop)

    assert pending_delivery.cancelled()
    assert completed_delivery.result() is True


@pytest.mark.asyncio
async def test_terminal_delivery_records_sink_failure(monkeypatch):
    terminal_event_by_field = {"run_id": "terminal-failure", "status": "failed"}
    delivery = asyncio.get_running_loop().create_future()
    status_events._publisher_state.terminal_event_by_run["terminal-failure"] = (
        terminal_event_by_field
    )
    status_events._publisher_state.terminal_delivery_by_run["terminal-failure"] = (
        delivery
    )
    status_events._publisher_state.pending_terminal_events.append(
        terminal_event_by_field
    )

    async def fail_post(_function, posted_event_by_field):
        assert posted_event_by_field is terminal_event_by_field
        raise OSError("sink unavailable")

    monkeypatch.setattr(status_events.asyncio, "to_thread", fail_post)
    worker_queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    worker = asyncio.create_task(status_events._publisher_worker(worker_queue))
    assert await asyncio.wait_for(asyncio.shield(delivery), timeout=0.25) is False
    worker.cancel()
    with suppress(asyncio.CancelledError):
        await worker


def test_terminal_delivery_ignores_stale_and_absent_state():
    stale_event_by_field = {"run_id": "stale", "status": "failed"}
    status_events._publisher_state.terminal_event_by_run["stale"] = {
        "run_id": "stale",
        "status": "failed",
    }
    status_events._resolve_terminal_delivery(stale_event_by_field, True)

    current_event_by_field = {"run_id": "current", "status": "failed"}
    status_events._publisher_state.terminal_event_by_run["current"] = (
        current_event_by_field
    )
    status_events._resolve_terminal_delivery(current_event_by_field, True)
