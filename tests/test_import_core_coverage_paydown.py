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


@pytest.mark.asyncio
async def test_status_event_bridge_delivers_worker_thread_and_prebound_events(
    monkeypatch,
):
    posted: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted.append)
    monkeypatch.setattr(status_events, "_throttle_seconds", lambda: 0.0)

    thread = threading.Thread(
        target=status_events.enqueue_status_event,
        args=({"run_id": "run-before-bind", "status": "running"},),
    )
    thread.start()
    thread.join()
    assert not posted

    status_events.bind_status_event_loop()
    await asyncio.to_thread(
        status_events.enqueue_status_event,
        {"run_id": "run-from-thread", "status": "running"},
    )
    await status_events.flush_status_events()

    assert [event["run_id"] for event in posted] == [
        "run-before-bind",
        "run-from-thread",
    ]


@pytest.mark.asyncio
async def test_status_event_bridge_worker_loop_cannot_steal_bound_owner(monkeypatch):
    posted: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted.append)
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
    assert [event["run_id"] for event in posted] == ["run-from-worker-loop"]


@pytest.mark.asyncio
async def test_status_event_bridge_coalesces_latest_progress_at_fixed_rate(
    monkeypatch,
):
    posted: list[dict[str, object]] = []
    monkeypatch.setattr(
        status_events, "_status_event_url", lambda: "https://sink.invalid/events"
    )
    monkeypatch.setattr(status_events, "_post_event", posted.append)
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

    assert [event["progress"]["event_seq"] for event in posted] == [1, 3]


@pytest.mark.asyncio
async def test_status_flush_queue_creation_and_worker_failures(monkeypatch):
    await status_events.flush_status_events()
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


@pytest.mark.parametrize(
    ("command", "targets"),
    [
        (main.stop_mrf.callback, ("process.MRF", "process.MRF_finish")),
        (
            main.stop_claims_pricing.callback,
            ("process.ClaimsPricing", "process.ClaimsPricing_finish"),
        ),
        (
            main.stop_claims_procedures.callback,
            ("process.ClaimsProcedures", "process.ClaimsProcedures_finish"),
        ),
        (
            main.stop_drug_claims.callback,
            ("process.DrugClaims", "process.DrugClaims_finish"),
        ),
        (
            main.stop_provider_enrichment.callback,
            ("process.ProviderEnrichment", "process.ProviderEnrichment_finish"),
        ),
        (
            main.stop_partd_formulary_network.callback,
            ("process.PartDFormularyNetwork", "process.PartDFormularyNetwork_finish"),
        ),
        (
            main.stop_pharmacy_license.callback,
            ("process.PharmacyLicense", "process.PharmacyLicense_finish"),
        ),
    ],
)
def test_stop_commands_control_import_override(monkeypatch, command, targets):
    calls: list[tuple[str, bool, dict[str, str]]] = []
    monkeypatch.setattr(
        main,
        "_run_worker_command",
        lambda target, burst, env: calls.append((target, burst, env)),
    )
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "stale")

    command(burst=False, import_id="run-22")
    command(burst=True, import_id=None)

    assert [call[0] for call in calls] == [
        targets[0],
        targets[1],
        targets[0],
        targets[1],
    ]
    assert calls[0][2]["HLTHPRT_IMPORT_ID_OVERRIDE"] == "run-22"
    assert "HLTHPRT_IMPORT_ID_OVERRIDE" not in calls[2][2]
    assert calls[0][1] is False
    assert calls[1][1] is True


def test_main_runtime_helpers_cover_error_and_command_paths(monkeypatch):
    monkeypatch.setenv("HLTHPRT_API_WORKERS", "invalid")
    monkeypatch.delenv("HLTHPRT_DB_ECHO", raising=False)
    monkeypatch.setattr(
        main,
        "connection",
        SimpleNamespace(
            _detect_server_capabilities=main.connection._detect_server_capabilities
        ),
    )
    assert main._default_api_workers() == 1
    assert main._job_id_text(b"job-1") == "job-1"
    assert main._job_id_text(22) == "22"

    run_calls: list[tuple[list[str], dict[str, str]]] = []
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda command, **kwargs: run_calls.append((command, kwargs)),
    )
    main._run_worker_command("process.Target", True, {"TOKEN": "one"})
    main._run_worker_command("process.Target", False, {"TOKEN": "two"})
    assert run_calls[0][0][-1] == "--burst"
    assert run_calls[1][0][-1] == "process.Target"

    monkeypatch.setattr("builtins.open", mock_open(read_data="{}"))
    monkeypatch.setattr(main.yaml, "safe_load", lambda _stream: {})
    monkeypatch.setattr(main.logging.config, "dictConfig", lambda _config: None)
    api_runs: list[dict[str, object]] = []
    monkeypatch.setattr(
        type(main.api), "run", lambda _api, **kwargs: api_runs.append(kwargs)
    )
    main.start.callback("127.0.0.1", 8081, 2, False, False)
    main.start.callback("127.0.0.1", 8082, 3, True, True)
    assert api_runs[1]["auto_reload"] is True
    assert main.os.environ["HLTHPRT_DB_ECHO"] == "True"


def test_main_cli_repairs_missing_or_non_uvloop_event_loop(monkeypatch):
    installed_loops: list[object] = []
    replacement = object()
    monkeypatch.setattr(main, "_new_event_loop", lambda: replacement)
    monkeypatch.setattr(main.asyncio, "set_event_loop", installed_loops.append)

    monkeypatch.setattr(
        main.asyncio, "get_event_loop", lambda: (_ for _ in ()).throw(RuntimeError())
    )
    main.cli.callback()

    current = SimpleNamespace(is_closed=lambda: False)
    monkeypatch.setattr(main.asyncio, "get_event_loop", lambda: current)
    main.cli.callback()

    closed = SimpleNamespace(is_closed=lambda: True)
    monkeypatch.setattr(main.asyncio, "get_event_loop", lambda: closed)
    main.cli.callback()
    assert installed_loops == [replacement, replacement, replacement]


def test_live_progress_recovers_previous_metadata_and_terminal_totals(monkeypatch):
    writes: list[tuple[str, int, str]] = []
    fake_redis = SimpleNamespace(
        setex=lambda key, ttl, value: writes.append((key, ttl, value))
    )
    previous_progress_by_field = {
        "importer": "provider-directory-fhir",
        "source": "source-progress",
        "confidence": "measured",
        "started_at": "2026-07-22T00:00:00Z",
    }
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_read_live_progress_payload",
        lambda _run_id: previous_progress_by_field,
    )
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


def test_live_progress_emits_central_event_when_local_redis_is_unavailable(
    monkeypatch,
):
    events: list[dict[str, object]] = []

    class FailingRedis:
        def get(self, _key):
            raise OSError("redis unavailable")

        def setex(self, *_args):
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

    assert events[0]["run_id"] == "run-without-redis"
    assert events[0]["progress"]["pct"] == 12


def test_live_progress_heartbeat_advances_observation_not_work(monkeypatch):
    stored: dict[str, str] = {}
    instants = iter(
        (
            dt.datetime(2026, 7, 23, 10, 0, 0),
            dt.datetime(2026, 7, 23, 10, 0, 15),
        )
    )

    class FakeRedis:
        def get(self, key):
            return stored.get(key)

        def setex(self, key, _ttl, value):
            stored[key] = value

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
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
    first = json.loads(stored["import:progress:run-heartbeat-v2"])
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
    heartbeat = json.loads(stored["import:progress:run-heartbeat-v2"])

    assert heartbeat["event_seq"] > first["event_seq"]
    assert heartbeat["progress_seq"] == first["progress_seq"]
    assert heartbeat["progressed_at"] == first["progressed_at"]
    assert heartbeat["observed_at"] > first["observed_at"]
    assert heartbeat["pct"] == 12


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

    estimate_from_start_by_field = {
        "done": 2,
        "total": 6,
        "started_at": now - dt.timedelta(seconds=4),
    }
    live_progress._normalize_estimate_fields(
        estimate_from_start_by_field, now=now, terminal=False
    )
    assert estimate_from_start_by_field["eta_seconds"] == 8

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
