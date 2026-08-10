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


def test_live_progress_recovers_previous_metadata_and_terminal_totals(monkeypatch):
    writes: list[tuple[str, int, str]] = []
    previous_progress_by_field = {
        "importer": "provider-directory-fhir",
        "source": "source-progress",
        "confidence": "measured",
        "started_at": "2026-07-22T00:00:00Z",
    }
    fake_redis = AtomicLiveProgressRedis(
        {
            "import:progress:run-live": json.dumps(
                previous_progress_by_field
            )
        },
        on_progress_write=lambda key, ttl, value: writes.append(
            (key, ttl, value)
        ),
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress, "_utc_now", lambda: dt.datetime(2026, 7, 22, 1, 0, 0)
    )

    live_progress.write_live_progress(
        run_id="run-live",
        importer="unknown",
        source="custom",
        status="succeeded",
        total=12,
        started_at="2026-07-22T00:30:00Z",
        label="https://example.test/path/file.ndjson",
        publish_event=False,
    )

    assert writes[0][0] == "import:progress:run-live"
    assert '"importer": "provider-directory-fhir"' in writes[0][2]
    assert '"done": 12' in writes[0][2]
    assert '"label": "example.test/file.ndjson"' in writes[0][2]


def test_live_progress_does_not_emit_unaccepted_event_when_redis_is_unavailable(
    monkeypatch,
):
    events: list[dict[str, object]] = []

    class FailingRedis:
        def set(self, *_args, **_kwargs):
            raise OSError("redis unavailable")

    monkeypatch.setattr(live_progress, "_redis", lambda: FailingRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", events.append)

    live_progress.write_live_progress(
        run_id="run-without-redis",
        importer="ptg",
        status="running",
        stage_id="scan",
        stage_ordinal=3,
        pct=12,
    )

    assert events == []


def test_live_progress_custom_event_tracks_the_accepted_attempt(monkeypatch):
    """Custom status events inherit the sequence accepted by Redis."""

    stored_by_key: dict[str, str] = {}
    events: list[dict[str, object]] = []
    fake_redis = AtomicLiveProgressRedis(stored_by_key)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: dt.datetime(2026, 7, 23, 9, 30, 0),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", events.append)

    accepted = live_progress.write_live_progress(
        run_id="run-custom-event",
        attempt_id="run-custom-event:attempt-2",
        attempt_started_at="2026-07-23T09:00:00Z",
        status="running",
        stage_id="scan",
        stage_ordinal=2,
        pct=25,
        status_event_payload={
            "run_id": "run-custom-event",
            "status": "running",
            "progress": {"contract": "retained"},
            "heartbeat_at": "2026-07-23T08:00:00Z",
        },
    )

    assert accepted is True
    stored_progress_by_field = json.loads(
        stored_by_key[live_progress.live_progress_key("run-custom-event")]
    )
    assert events == [
        {
            "run_id": "run-custom-event",
            "status": "running",
            "progress": {
                "contract": "retained",
                "attempt_id": stored_progress_by_field["attempt_id"],
                "attempt_started_at": stored_progress_by_field[
                    "attempt_started_at"
                ],
                "event_seq": stored_progress_by_field["event_seq"],
                "progress_seq": stored_progress_by_field["progress_seq"],
            },
            "heartbeat_at": stored_progress_by_field["observed_at"],
        }
    ]


def test_live_progress_heartbeat_advances_observation_not_work(monkeypatch):
    stored_by_key: dict[str, str] = {}
    instants = iter(
        (
            dt.datetime(2026, 7, 23, 10, 0, 0),
            dt.datetime(2026, 7, 23, 10, 0, 15),
        )
    )

    fake_redis = AtomicLiveProgressRedis(stored_by_key)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(live_progress, "_utc_now", lambda: next(instants))
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)

    live_progress.write_live_progress(
        run_id="run-heartbeat-v2",
        importer="ptg",
        status="running",
        source="ptg2-scanner-progress",
        stage_id="scan",
        stage_ordinal=3,
        pct=12,
    )
    first = json.loads(stored_by_key["import:progress:run-heartbeat-v2"])
    live_progress.write_live_progress(
        run_id="run-heartbeat-v2",
        importer="ptg",
        status="running",
        source="engine-heartbeat",
        phase="ptg_control_start running",
        unit="run",
        done=0,
        total=1,
        pct=0,
    )
    heartbeat = json.loads(stored_by_key["import:progress:run-heartbeat-v2"])

    assert heartbeat["event_seq"] > first["event_seq"]
    assert heartbeat["progress_seq"] == first["progress_seq"]
    assert heartbeat["progressed_at"] == first["progressed_at"]
    assert heartbeat["observed_at"] > first["observed_at"]
    assert heartbeat["pct"] == 12


def test_live_progress_new_attempt_resets_progress_and_sequences(monkeypatch):
    stored_by_key: dict[str, str] = {}
    instants = iter(
        (
            dt.datetime(2026, 7, 23, 10, 0, 10),
            dt.datetime(2026, 7, 23, 11, 0, 4),
        )
    )

    fake_redis = AtomicLiveProgressRedis(stored_by_key)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(live_progress, "_utc_now", lambda: next(instants))

    run_id = "run-new-attempt-reset"
    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:first",
        attempt_started_at="2026-07-23T10:00:00Z",
        started_at="2026-07-23T10:00:00Z",
        status="failed",
        stage_id="scan",
        stage_ordinal=3,
        phase="scanning",
        done=80,
        total=100,
        pct=80,
        counters={"groups": 99},
        publish_event=False,
    )
    first = json.loads(stored_by_key[live_progress.live_progress_key(run_id)])
    assert first["event_seq"] == 1
    assert first["progress_seq"] == 1

    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:second",
        attempt_started_at="2026-07-23T11:00:00Z",
        started_at="2026-07-23T11:00:00Z",
        phase="restarting",
        done=1,
        total=4,
        pct=25,
        publish_event=False,
    )
    restarted = json.loads(stored_by_key[live_progress.live_progress_key(run_id)])

    assert restarted["attempt_id"] == f"{run_id}:second"
    assert restarted["attempt_started_at"] == "2026-07-23T11:00:00Z"
    assert restarted["started_at"] == "2026-07-23T10:00:00Z"
    assert restarted["status"] == "running"
    assert restarted["phase"] == "restarting"
    assert restarted["pct"] == 25
    assert restarted["eta_seconds"] == 12
    assert restarted["event_seq"] == 1
    assert restarted["progress_seq"] == 1
    assert "stage_id" not in restarted
    assert "stage_ordinal" not in restarted
    assert "counters" not in restarted


def test_live_progress_same_attempt_keeps_ordinal_and_sequence_fences(monkeypatch):
    stored_by_key: dict[str, str] = {}
    instants = iter(
        (
            dt.datetime(2026, 7, 23, 10, 0, 0),
            dt.datetime(2026, 7, 23, 10, 0, 1),
        )
    )

    fake_redis = AtomicLiveProgressRedis(stored_by_key)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(live_progress, "_utc_now", lambda: next(instants))

    run_id = "run-same-attempt-fence"
    attempt_id = f"{run_id}:current"
    attempt_started_at = "2026-07-23T09:00:00Z"
    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
        stage_id="publish",
        stage_ordinal=5,
        pct=90,
        publish_event=False,
    )
    first = json.loads(stored_by_key[live_progress.live_progress_key(run_id)])

    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
        stage_id="scan",
        stage_ordinal=3,
        pct=10,
        publish_event=False,
    )
    fenced = json.loads(stored_by_key[live_progress.live_progress_key(run_id)])

    assert fenced["attempt_id"] == attempt_id
    assert fenced["stage_id"] == "publish"
    assert fenced["stage_ordinal"] == 5
    assert fenced["pct"] == 90
    assert fenced["event_seq"] == first["event_seq"] + 1
    assert fenced["progress_seq"] == first["progress_seq"]


def test_live_progress_rejects_delayed_older_attempt(monkeypatch):
    stored_by_key: dict[str, str] = {}
    write_counts = [0]

    def count_write(_key, _ttl, _value):
        write_counts[0] += 1

    fake_redis = AtomicLiveProgressRedis(
        stored_by_key,
        on_progress_write=count_write,
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: dt.datetime(2026, 7, 23, 12, 0, 0),
    )

    run_id = "run-reject-old-attempt"
    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:current",
        attempt_started_at="2026-07-23T11:00:00Z",
        status="running",
        stage_id="publish",
        stage_ordinal=5,
        pct=90,
        publish_event=False,
    )
    current_payload = stored_by_key[live_progress.live_progress_key(run_id)]

    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:old",
        attempt_started_at="2026-07-23T10:00:00Z",
        status="failed",
        stage_id="scan",
        stage_ordinal=3,
        pct=10,
        publish_event=False,
    )

    assert write_counts[0] == 1
    assert stored_by_key[live_progress.live_progress_key(run_id)] == current_payload


@pytest.mark.parametrize("attempt_started_at", [None, "not-a-timestamp"])
def test_live_progress_rejects_unordered_attempt_against_timestamped_current(
    monkeypatch,
    attempt_started_at,
):
    stored_by_key: dict[str, str] = {}

    fake_redis = AtomicLiveProgressRedis(stored_by_key)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: dt.datetime(2026, 7, 23, 12, 0, 0),
    )

    run_id = f"run-unordered-attempt-{attempt_started_at}"
    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:current",
        attempt_started_at="2026-07-23T11:00:00Z",
        stage_id="publish",
        stage_ordinal=5,
        pct=90,
        publish_event=False,
    )
    current_payload = stored_by_key[live_progress.live_progress_key(run_id)]

    live_progress.write_live_progress(
        run_id=run_id,
        attempt_id=f"{run_id}:unknown",
        attempt_started_at=attempt_started_at,
        status="failed",
        pct=1,
        publish_event=False,
    )

    assert stored_by_key[live_progress.live_progress_key(run_id)] == current_payload


def test_live_progress_attempt_ordering_fails_closed_for_malformed_named_current():
    run_id = "run-malformed-current-attempt"

    assert (
        live_progress._attempt_disposition(
            {
                "run_id": run_id,
                "attempt_id": f"{run_id}:incoming",
                "attempt_started_at": "2026-07-23T12:00:00Z",
            },
            {
                "run_id": run_id,
                "attempt_id": f"{run_id}:current",
                "attempt_started_at": "invalid",
            },
        )
        == live_progress._ATTEMPT_REJECT
    )


def test_live_progress_attempt_ordering_accepts_timestamped_run_id_alias():
    run_id = "run-thread-attempt-alias"
    attempt_started_at = "2026-07-23T12:00:00Z"

    assert (
        live_progress._attempt_disposition(
            {
                "run_id": run_id,
                "attempt_id": run_id,
                "started_at": attempt_started_at,
            },
            {
                "run_id": run_id,
                "attempt_id": f"{run_id}:{attempt_started_at}",
                "attempt_started_at": attempt_started_at,
            },
        )
        == live_progress._ATTEMPT_CURRENT
    )
