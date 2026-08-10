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
from tests.test_import_core_coverage_paydown import _LoopProbe

@pytest.fixture(autouse=True)
def _reset_status_publisher():
    status_events._publisher_state.queue = None
    status_events._publisher_state.worker = None
    status_events._publisher_state.loop = None
    status_events._publisher_state.pending.clear()
    status_events._publisher_state.coalesced_by_run.clear()
    status_events._publisher_state.flush_handle_by_run.clear()
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
    status_events._last_sent_by_run.clear()


class _LiveProgressCasRaceHarness:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.key = live_progress.live_progress_key(run_id)
        self.storage_by_key: dict[str, object] = {}
        self.shared_lock = threading.Lock()
        self.first_reads = threading.Barrier(2)
        self.events: list[dict[str, object]] = []
        self.event_lock = threading.Lock()
        self.write_results: list[bool] = []
        self.clients = (self._new_client(), self._new_client())

    def _new_client(self) -> AtomicLiveProgressRedis:
        wait_flags = [False]

        def before_get(candidate_key):
            if candidate_key == self.key and not wait_flags[0]:
                wait_flags[0] = True
                self.first_reads.wait(timeout=2)

        return AtomicLiveProgressRedis(
            self.storage_by_key,
            before_get=before_get,
            shared_lock=self.shared_lock,
        )

    def capture_event(self, status_event: dict[str, object]) -> None:
        with self.event_lock:
            self.events.append(status_event)

    def write_progress(
        self,
        redis_client: AtomicLiveProgressRedis,
        attempt_id: str,
        attempt_started_at: str,
    ) -> None:
        self.write_results.append(
            live_progress._write_live_progress_with_cas(
                redis_client=redis_client,
                run_id=self.run_id,
                context={},
                payload={
                    "attempt_id": attempt_id,
                    "attempt_started_at": attempt_started_at,
                    "status": "running",
                    "stage_id": "scan",
                    "stage_ordinal": 1,
                    "pct": 5,
                },
                observed_at="2026-07-23T12:00:01Z",
                now=dt.datetime(2026, 7, 23, 12, 0, 1),
                status_event_payload=None,
            )
        )

    def run_threads(self) -> tuple[threading.Thread, threading.Thread]:
        old_thread = threading.Thread(
            target=self.write_progress,
            args=(self.clients[0], f"{self.run_id}:old", "2026-07-23T10:00:00Z"),
        )
        new_thread = threading.Thread(
            target=self.write_progress,
            args=(self.clients[1], f"{self.run_id}:new", "2026-07-23T11:00:00Z"),
        )
        old_thread.start()
        new_thread.start()
        old_thread.join(timeout=3)
        new_thread.join(timeout=3)
        return old_thread, new_thread


def test_live_progress_cas_two_clients_converges_on_newer_attempt(
    monkeypatch,
):
    """Two stale readers cannot overwrite the newer attempt after CAS."""

    run_id = "run-two-client-cas"
    harness = _LiveProgressCasRaceHarness(run_id)
    monkeypatch.setattr(
        live_progress,
        "enqueue_status_event",
        harness.capture_event,
    )
    old_thread, new_thread = harness.run_threads()

    assert not old_thread.is_alive()
    assert not new_thread.is_alive()
    retained = json.loads(str(harness.storage_by_key[harness.key]))
    assert retained["attempt_id"] == f"{run_id}:new"
    event_attempts = [
        event["progress"]["attempt_id"]
        for event in harness.events
    ]
    assert event_attempts[-1] == f"{run_id}:new"
    if f"{run_id}:old" in event_attempts:
        assert event_attempts.index(f"{run_id}:old") < event_attempts.index(
            f"{run_id}:new"
        )
    assert any(harness.write_results)


class _LiveProgressAttemptGateHarness:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.storage_by_key: dict[str, object] = {}
        self.shared_lock = threading.Lock()
        self.old_event_entered = threading.Event()
        self.release_old_event = threading.Event()
        self.new_lock_attempted = threading.Event()
        self.events: list[str] = []
        self.thread_client = threading.local()
        self.write_result_by_label: dict[str, bool] = {}
        self.clients = (
            AtomicLiveProgressRedis(self.storage_by_key, shared_lock=self.shared_lock),
            self._new_lock_observing_client(),
        )
        self.attempt_by_label = {
            "old": (self.clients[0], "2026-07-23T10:00:00Z", "failed"),
            "new": (self.clients[1], "2026-07-23T11:00:00Z", "running"),
        }

    def _new_lock_observing_client(self) -> AtomicLiveProgressRedis:
        return AtomicLiveProgressRedis(
            self.storage_by_key,
            before_set=lambda key: (
                self.new_lock_attempted.set()
                if key == live_progress._progress_publication_lock_key(self.run_id)
                else None
            ),
            shared_lock=self.shared_lock,
        )

    def redis_for_thread(self) -> AtomicLiveProgressRedis:
        return self.thread_client.value

    def capture_event(self, status_event: dict[str, object]) -> None:
        attempt_id = status_event["progress"]["attempt_id"]
        if attempt_id == f"{self.run_id}:old":
            self.old_event_entered.set()
            assert self.release_old_event.wait(timeout=2)
        self.events.append(attempt_id)

    def write_progress(self, label: str) -> None:
        client, started_at, status = self.attempt_by_label[label]
        self.thread_client.value = client
        self.write_result_by_label[label] = live_progress.write_live_progress(
            run_id=self.run_id,
            attempt_id=f"{self.run_id}:{label}",
            attempt_started_at=started_at,
            status=status,
            pct=90 if label == "old" else 1,
        )

    def old_thread(self) -> threading.Thread:
        return threading.Thread(target=self.write_progress, args=("old",))

    def new_thread(self) -> threading.Thread:
        return threading.Thread(target=self.write_progress, args=("new",))


def test_live_progress_holds_attempt_gate_through_terminal_event_enqueue(
    monkeypatch,
):
    """A newer claim waits until an accepted older event is already queued."""

    run_id = "run-event-order-gate"
    harness = _LiveProgressAttemptGateHarness(run_id)
    storage_by_key = harness.storage_by_key
    old_event_entered = harness.old_event_entered
    release_old_event = harness.release_old_event
    new_lock_attempted = harness.new_lock_attempted
    events = harness.events
    write_result_by_label = harness.write_result_by_label

    monkeypatch.setattr(live_progress, "_redis", harness.redis_for_thread)
    monkeypatch.setattr(
        live_progress,
        "_progress_lock_for",
        lambda _run_id: threading.Lock(),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", harness.capture_event)
    old_thread = harness.old_thread()
    new_thread = harness.new_thread()

    old_thread.start()
    assert old_event_entered.wait(timeout=2)
    new_thread.start()
    assert new_lock_attempted.wait(timeout=2)
    assert new_thread.is_alive()
    assert events == []

    release_old_event.set()
    old_thread.join(timeout=3)
    new_thread.join(timeout=3)

    assert not old_thread.is_alive()
    assert not new_thread.is_alive()
    assert write_result_by_label == {"old": True, "new": True}
    assert events == [f"{run_id}:old", f"{run_id}:new"]
    retained = json.loads(
        str(storage_by_key[live_progress.live_progress_key(run_id)])
    )
    assert retained["attempt_id"] == f"{run_id}:new"


def test_live_progress_scheduling_reads_and_parsing_edges(monkeypatch):
    read_payload = live_progress._read_live_progress_payload
    writes: list[dict[str, object]] = []
    monkeypatch.setattr(
        live_progress, "write_live_progress", lambda **payload: writes.append(payload)
    )
    live_progress.enqueue_live_progress(run_id="sync")
    assert writes == [{"run_id": "sync"}]

    failing_loop = _LoopProbe(fail_create=True)
    monkeypatch.setattr(live_progress.asyncio, "get_running_loop", lambda: failing_loop)
    live_progress.enqueue_live_progress(run_id="failed-schedule")

    now = dt.datetime(2026, 7, 22, 2, 0, 0)
    monkeypatch.setattr(live_progress, "_utc_now", lambda: now)
    monkeypatch.setattr(
        live_progress, "_read_live_progress_payload", lambda _run_id: {"value": 1}
    )
    assert live_progress.read_live_progress("") is None
    assert live_progress.read_live_progress("run-no-time") == {"value": 1}
    monkeypatch.setattr(
        live_progress,
        "_read_live_progress_payload",
        lambda _run_id: {"updated_at": "2026-07-22T00:00:00Z"},
    )
    assert live_progress.read_live_progress("stale") is None

    fake_redis = SimpleNamespace(get=lambda _key: b"not-json")
    monkeypatch.setattr(live_progress, "_read_live_progress_payload", read_payload)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    assert live_progress._read_live_progress_payload("invalid") is None
    fake_redis.get = lambda _key: "[]"
    assert live_progress._read_live_progress_payload("list") is None


def test_live_progress_normalization_and_safe_display_edges():
    heartbeat_by_field = {"source": "engine-heartbeat", "done": 0}
    live_progress._preserve_progress_for_heartbeat(
        heartbeat_by_field,
        {"source": "engine-heartbeat", "done": 9},
        now=dt.datetime(2026, 7, 22),
    )
    assert heartbeat_by_field["done"] == 0

    estimate_by_field = {"done": 2, "total": 6, "elapsed_seconds": 4}
    now = dt.datetime(2026, 7, 22)
    live_progress._normalize_estimate_fields(estimate_by_field, now=now, terminal=False)
    assert estimate_by_field["eta_seconds"] == 8

    estimate_from_attempt_start_by_field = {
        "done": 2,
        "total": 6,
        "started_at": now - dt.timedelta(hours=1),
        "attempt_started_at": now - dt.timedelta(seconds=4),
    }
    live_progress._normalize_estimate_fields(
        estimate_from_attempt_start_by_field,
        now=now,
        terminal=False,
    )
    assert estimate_from_attempt_start_by_field["eta_seconds"] == 8

    assert (
        live_progress._safe_label("https://example.test/path/file.ndjson")
        == "example.test/file.ndjson"
    )
    assert live_progress._safe_label("https://example.test") == "example.test"
    assert live_progress._safe_label("plain") == "plain"
    aware = dt.datetime(2026, 7, 22, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert live_progress._parse_datetime(aware).tzinfo is None
    assert live_progress._parse_datetime("invalid") is None


def test_live_progress_sequence_cache_is_bounded():
    cache = live_progress.OrderedDict()
    limit = live_progress._SEQUENCE_CACHE_MAX_PER_STRIPE

    for index in range(limit + 3):
        live_progress._remember_sequence(cache, f"run-{index}", index)

    assert len(cache) == limit
    assert "run-0" not in cache
    assert cache[f"run-{limit + 2}"] == limit + 2
