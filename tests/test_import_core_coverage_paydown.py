# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import json
import threading
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import mock_open

import pytest

import main
from process import import_status_events as status_events
from process import live_progress
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis


class _QueueProbe:
    def __init__(
        self,
        *,
        full: bool = False,
        empty_on_get: bool = False,
        full_on_put: bool = False,
    ):
        self._full = full
        self._empty_on_get = empty_on_get
        self._full_on_put = full_on_put
        self.items: list[dict[str, object]] = []
        self.completed = 0

    def full(self) -> bool:
        return self._full

    def get_nowait(self) -> dict[str, object]:
        if self._empty_on_get:
            raise asyncio.QueueEmpty
        return self.items.pop(0) if self.items else {}

    def task_done(self) -> None:
        self.completed += 1

    def put_nowait(self, item: dict[str, object]) -> None:
        if self._full_on_put:
            raise asyncio.QueueFull
        self.items.append(item)


class _LoopProbe:
    def __init__(self, *, fail_create: bool = False):
        self.fail_create = fail_create
        self.created = 0
        self.timers: list[SimpleNamespace] = []

    def create_task(self, coroutine):
        self.created += 1
        coroutine.close()
        if self.fail_create:
            raise RuntimeError("task scheduling failed")
        return SimpleNamespace(done=lambda: False)

    def create_future(self):
        future = SimpleNamespace(value=None)
        future.done = lambda: future.value is not None
        future.set_result = lambda value: setattr(future, "value", value)
        future.cancel = lambda: setattr(future, "value", False)
        return future

    def call_soon(self, callback, *args):
        callback(*args)

    def call_later(self, _delay, callback, *args):
        timer = SimpleNamespace(
            cancelled_value=False,
            cancel=lambda: setattr(timer, "cancelled_value", True),
            cancelled=lambda: timer.cancelled_value,
            fire=lambda: callback(*args),
        )
        self.timers.append(timer)
        return timer


@pytest.fixture(autouse=True)
def _reset_status_publisher():
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    status_events._publisher_state.loop = None
    status_events._publisher_state.pending.clear()
    status_events._publisher_state.coalesced_by_run.clear()
    status_events._publisher_state.flush_handle_by_run.clear()
    status_events._publisher_state.pending_terminal_events.clear()
    status_events._publisher_state.terminal_event_by_run.clear()
    status_events._publisher_state.terminal_delivery_by_run.clear()
    status_events._last_sent_by_run.clear()
    yield
    worker = status_events._publisher_state.worker
    if isinstance(worker, asyncio.Task):
        worker.cancel()
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    status_events._publisher_state.loop = None
    status_events._publisher_state.pending.clear()
    status_events._publisher_state.coalesced_by_run.clear()
    status_events._publisher_state.flush_handle_by_run.clear()
    status_events._publisher_state.pending_terminal_events.clear()
    status_events._publisher_state.terminal_event_by_run.clear()
    status_events._publisher_state.terminal_delivery_by_run.clear()
    status_events._last_sent_by_run.clear()


def test_status_serialization_and_sink_settings(monkeypatch):
    naive = dt.datetime(2026, 7, 22, 1, 2, 3)
    aware = naive.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))

    assert status_events.isoformat_utc(" ") == " "
    assert status_events.isoformat_utc("not-a-date") == "not-a-date"
    assert status_events.isoformat_utc(naive).endswith("+00:00")
    assert status_events.isoformat_utc(aware).startswith("2026-07-21T23:02:03")
    assert status_events.isoformat_utc(7) == 7

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "node-7")
    event = status_events._event_payload(
        {
            "run_id": "run-7",
            "created_at": naive,
            "started_at": None,
            "finished_at": "bad",
        }
    )
    assert event["node_id"] == "node-7"
    assert event["created_at"].endswith("+00:00")
    assert event["finished_at"] == "bad"

    monkeypatch.delenv("HLTHPRT_IMPORT_STATUS_EVENT_TOKEN", raising=False)
    assert status_events._auth_headers() == {}
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_TOKEN", " secret ")
    assert status_events._auth_headers() == {"Authorization": "Bearer secret"}
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_THROTTLE_SECONDS", "-1")
    assert status_events._timeout_seconds() == 0.1
    assert status_events._throttle_seconds() == 0.0


def test_status_enqueue_filters_throttles_and_bounds_queue(monkeypatch):
    loop = _LoopProbe()
    queue = _QueueProbe()
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: queue)
    monkeypatch.setattr(status_events.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 30.0)

    status_events.enqueue_status_event({"status": "running"})
    status_events.enqueue_status_event(
        {"run_id": "run-1", "status": "running", "phase_detail": "read"}
    )
    status_events.enqueue_status_event(
        {"run_id": "run-1", "status": "running", "phase_detail": "read"}
    )
    status_events.enqueue_status_event({"run_id": "run-1", "status": "succeeded"})

    assert [event["status"] for event in queue.items] == ["running", "succeeded"]

    evicting_queue = _QueueProbe(full=True)
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: evicting_queue)
    status_events.enqueue_status_event({"run_id": "run-2", "status": "succeeded"})
    assert evicting_queue.completed == 1
    assert evicting_queue.items[0]["run_id"] == "run-2"

    saturated_queue = _QueueProbe(full=True, empty_on_get=True, full_on_put=True)
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: saturated_queue)
    status_events.enqueue_status_event({"run_id": "run-3", "status": "succeeded"})
    assert saturated_queue.items == []


def test_status_enqueue_handles_absent_sink_and_event_loop(monkeypatch):
    monkeypatch.setattr(status_events, "_status_event_url", lambda: "")
    status_events.enqueue_status_event({"run_id": "run-1", "status": "running"})

    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )

    def no_running_loop():
        raise RuntimeError("no loop")

    monkeypatch.setattr(status_events.asyncio, "get_running_loop", no_running_loop)
    status_events.enqueue_status_event({"run_id": "run-1", "status": "succeeded"})


def test_status_publisher_recovers_stale_loops_and_flushes_edge_states(monkeypatch):
    """Preserve events across stale owners, full pending buffers, and loop changes."""

    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )

    def no_running_loop():
        raise RuntimeError("no loop")

    monkeypatch.setattr(status_events.asyncio, "get_running_loop", no_running_loop)
    status_events.bind_status_event_loop()

    stale_loop = SimpleNamespace(is_running=lambda: False)
    status_events._publisher_state.loop = stale_loop
    status_events.enqueue_status_event(
        {"run_id": "run-stale", "status": "running"}
    )
    assert status_events._publisher_state.pending[-1]["run_id"] == "run-stale"

    class RejectingLoop:
        @staticmethod
        def is_running():
            return True

        @staticmethod
        def call_soon_threadsafe(*_args):
            raise RuntimeError("loop closed")

    current_loop = object()
    status_events._publisher_state.loop = RejectingLoop()
    monkeypatch.setattr(
        status_events.asyncio,
        "get_running_loop",
        lambda: current_loop,
    )
    status_events.enqueue_status_event(
        {"run_id": "run-rejected", "status": "running"}
    )
    assert status_events._publisher_state.pending[-1]["run_id"] == "run-rejected"

    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "1")
    status_events._append_pending_event_locked({"status": "missing-run"})
    status_events._append_pending_event_locked(
        {"run_id": "run-bounded", "status": "running"}
    )
    assert list(status_events._publisher_state.pending) == [
        {"run_id": "run-bounded", "status": "running"}
    ]


def test_status_publisher_flushes_missing_and_loop_change_states(monkeypatch):
    """Flush coalesced events and cancel timers owned by a replaced loop."""

    queue = _QueueProbe()
    loop = _LoopProbe()
    real_ensure_queue = status_events._ensure_queue
    monkeypatch.setattr(status_events, "_ensure_queue", lambda _loop: queue)
    status_events._accept_event_on_loop(loop, {})
    status_events._publish_event_now(queue, {})
    status_events._flush_coalesced_event(loop, "missing")

    cancelled_run_ids: list[str] = []
    status_events._publisher_state.coalesced_by_run.update(
        {
            "run-handled": {"run_id": "run-handled", "status": "running"},
            "run-unhandled": {"run_id": "run-unhandled", "status": "running"},
        }
    )
    status_events._publisher_state.flush_handle_by_run["run-handled"] = (
        SimpleNamespace(cancel=lambda: cancelled_run_ids.append("run-handled"))
    )
    status_events._flush_all_coalesced(loop)
    assert cancelled_run_ids == ["run-handled"]
    assert [event["run_id"] for event in queue.items[-2:]] == [
        "run-handled",
        "run-unhandled",
    ]

    class MissingFirstEvent(dict):
        def pop(self, key, default=None):
            if key == "run-missing":
                return None
            return super().pop(key, default)

    status_events._publisher_state.coalesced_by_run = MissingFirstEvent(
        {
            "run-missing": {},
            "run-present": {"run_id": "run-present", "status": "running"},
        }
    )
    status_events._flush_all_coalesced(loop)
    assert queue.items[-1]["run_id"] == "run-present"

    old_handle = SimpleNamespace(cancel=lambda: cancelled_run_ids.append("old-loop"))
    status_events._publisher_state.loop = SimpleNamespace(is_running=lambda: False)
    status_events._publisher_state.flush_handle_by_run["old-loop"] = old_handle
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "1")
    monkeypatch.setattr(
        status_events,
        "_ensure_queue",
        real_ensure_queue,
    )
    status_events._ensure_queue(_LoopProbe())
    assert cancelled_run_ids[-1] == "old-loop"


@pytest.mark.asyncio
async def test_status_event_bridge_delivers_worker_thread_and_prebound_events(
    monkeypatch,
):
    posted_events: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted_events.append)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 0.0)

    thread = threading.Thread(
        target=status_events.enqueue_status_event,
        args=({"run_id": "run-before-bind", "status": "running"},),
    )
    thread.start()
    thread.join()
    assert not posted_events

    status_events.bind_status_event_loop()
    await asyncio.to_thread(
        status_events.enqueue_status_event,
        {"run_id": "run-from-thread", "status": "running"},
    )
    await status_events.flush_status_events()

    assert [event["run_id"] for event in posted_events] == [
        "run-before-bind",
        "run-from-thread",
    ]


@pytest.mark.asyncio
async def test_status_event_bridge_worker_loop_cannot_steal_bound_owner(monkeypatch):
    posted_events: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted_events.append)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 0.0)
    status_events.bind_status_event_loop()
    owner_loop = status_events._publisher_state.loop

    async def worker_loop() -> None:
        status_events.bind_status_event_loop()
        status_events.enqueue_status_event(
            {"run_id": "run-from-worker-loop", "status": "running"}
        )

    await asyncio.to_thread(lambda: asyncio.run(worker_loop()))
    await status_events.flush_status_events()

    assert status_events._publisher_state.loop is owner_loop
    assert [event["run_id"] for event in posted_events] == ["run-from-worker-loop"]


@pytest.mark.asyncio
async def test_status_event_bridge_coalesces_latest_progress_at_fixed_rate(
    monkeypatch,
):
    posted_events: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted_events.append)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 0.02)
    status_events.bind_status_event_loop()

    for event_seq in (1, 2, 3):
        status_events.enqueue_status_event(
            {
                "run_id": "run-coalesced",
                "status": "running",
                "phase_detail": "scan",
                "progress": {"event_seq": event_seq, "pct": event_seq},
            }
        )
    await asyncio.sleep(0.03)
    await status_events.flush_status_events()

    assert [event["progress"]["event_seq"] for event in posted_events] == [1, 3]


@pytest.mark.asyncio
async def test_status_flush_queue_creation_and_worker_failures(monkeypatch):
    await status_events.flush_status_events()
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    queue = asyncio.Queue()
    status_events._publisher_state.queue = queue
    await status_events.flush_status_events()

    async def timeout_wait(_awaitable, *, timeout):
        assert timeout == 0.25
        _awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(status_events.asyncio, "wait_for", timeout_wait)
    await status_events.flush_status_events(0.25)

    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_QUEUE_SIZE", "0")
    probe = _LoopProbe()
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    created_queue = status_events._ensure_queue(probe)
    assert created_queue.maxsize == 1
    assert status_events._ensure_queue(probe) is created_queue
    status_events._publisher_state.worker = SimpleNamespace(done=lambda: True)
    status_events._ensure_queue(probe)
    assert probe.created == 2

    attempts: list[str] = []

    async def publish_in_thread(_function, event):
        attempts.append(event["run_id"])
        if len(attempts) == 1:
            raise OSError("sink unavailable")

    worker_queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    monkeypatch.setattr(status_events.asyncio, "to_thread", publish_in_thread)
    task = asyncio.create_task(status_events._publisher_worker(worker_queue))
    worker_queue.put_nowait({"run_id": "one", "status": "running"})
    worker_queue.put_nowait({"run_id": "two", "status": "succeeded"})
    await worker_queue.join()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert attempts == ["one", "two"]


def test_status_post_builds_request_and_reads_response(monkeypatch):
    monkeypatch.delenv("HLTHPRT_IMPORT_STATUS_EVENT_URL", raising=False)
    monkeypatch.delenv("HLTHPRT_IMPORT_STATUS_EVENT_TOKEN", raising=False)
    status_events._post_event({"run_id": "ignored"})
    captured_by_name: dict[str, object] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            captured_by_name["read"] = True
            return b""

    def open_request(request, *, timeout):
        captured_by_name["request"] = request
        captured_by_name["timeout"] = timeout
        return Response()

    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_URL", "https://sink.invalid/events")
    monkeypatch.setenv("HLTHPRT_IMPORT_STATUS_EVENT_TOKEN", "token")
    monkeypatch.setattr(status_events.urllib.request, "urlopen", open_request)
    status_events._post_event(
        {"run_id": "run-9", "created_at": dt.datetime(2026, 7, 22)}
    )

    request = captured_by_name["request"]
    assert request.full_url == "https://sink.invalid/events"
    assert request.get_method() == "POST"
    assert request.get_header("Authorization") == "Bearer token"
    assert captured_by_name["read"] is True
