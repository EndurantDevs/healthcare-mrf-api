# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime as dt

import pytest

from process.control_cancel import ImportCancelledError, raise_if_cancelled, run_id_from_task
from process import control_lifecycle
from process.control_lifecycle import control_single_job_start
from process.live_progress import current_live_progress_context


_SHUTDOWN_METRICS_BY_NAME = {
    "staged_rows": 123,
    "source_table_shards": 64,
    "stage_index_profile": "serving",
    "post_publish_index_profile": "serving",
    "post_publish_index_concurrently": True,
    "post_publish_index_pending": False,
    "post_publish_index_total": 2,
    "post_publish_index_completed": 2,
    "post_publish_index_timings": [
        {"index": "geo_bbox", "seconds": 13.4},
        {"index": "geo_idx", "seconds": 28.1},
    ],
    "post_publish_skipped_indexes": [],
    "published_elapsed_seconds": 233.2,
    "phase_timings": {
        "entity-address-unified indexing stage": {"wall_seconds": 24.1}
    },
    "stage_index_timings": [{"index": "primary_npi", "seconds": 1.2}],
    "preserve_control_run_finished_at": True,
}


def _assert_shutdown_terminal_metrics(terminal):
    metrics_by_name = terminal["metrics"]
    assert metrics_by_name["rows"] == metrics_by_name["staged_rows"] == 123
    assert metrics_by_name["source_table_shards"] == 64
    assert metrics_by_name["stage_index_profile"] == "serving"
    assert metrics_by_name["post_publish_index_profile"] == "serving"
    assert metrics_by_name["post_publish_index_concurrently"] is True
    assert metrics_by_name["post_publish_index_pending"] is False
    assert metrics_by_name["post_publish_index_total"] == 2
    assert metrics_by_name["post_publish_index_completed"] == 2
    assert metrics_by_name["post_publish_index_timings"][0]["index"] == "geo_bbox"
    assert metrics_by_name["post_publish_index_timings"][1]["index"] == "geo_idx"
    assert metrics_by_name["post_publish_skipped_indexes"] == []
    assert metrics_by_name["published_elapsed_seconds"] == 233.2
    assert metrics_by_name["phase_timings"]["entity-address-unified indexing stage"] == {
        "wall_seconds": 24.1
    }
    assert metrics_by_name["stage_index_timings"][0]["index"] == "primary_npi"
    assert terminal["progress"]["done"] == 123
    assert terminal["preserve_finished_at"] is True


def test_run_id_from_task_normalizes_missing_values():
    assert run_id_from_task(None) is None
    assert run_id_from_task({}) is None
    assert run_id_from_task({"run_id": " run_1 "}) == "run_1"


@pytest.mark.asyncio
async def test_raise_if_cancelled_noops_without_run_id():
    await raise_if_cancelled({"redis": object()}, {})


@pytest.mark.asyncio
async def test_raise_if_cancelled_raises_for_redis_flag():
    class FakeRedis:
        async def get(self, key):
            assert key == "cancel:run_1"
            return b"1"

    with pytest.raises(ImportCancelledError):
        await raise_if_cancelled({"redis": FakeRedis()}, {"run_id": "run_1"})


@pytest.mark.asyncio
async def test_raise_if_cancelled_allows_missing_flag():
    class FakeRedis:
        async def get(self, _key):
            return None

    await raise_if_cancelled({"redis": FakeRedis()}, {"run_id": "run_1"})


@pytest.mark.asyncio
async def test_control_single_job_start_marks_success(monkeypatch):
    marks = []
    calls = []
    live_contexts = []
    real_set_live_progress_context = control_lifecycle.set_live_progress_context

    async def is_control_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    def capture_live_progress_context(**payload):
        live_contexts.append(payload)
        return real_set_live_progress_context(**payload)

    monkeypatch.setattr(control_lifecycle, "set_live_progress_context", capture_live_progress_context)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_control_run_marked)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    run_response = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
    )

    assert run_response["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [mark_entry[1]["status"] for mark_entry in marks] == ["running", "succeeded"]
    assert live_contexts == [
        {
            "run_id": "run_1",
            "importer": "process_data",
            "status": "running",
            "started_at": live_contexts[0]["started_at"],
            "attempt_id": live_contexts[0]["attempt_id"],
            "attempt_started_at": live_contexts[0]["started_at"],
        }
    ]
    assert live_contexts[0]["attempt_id"].startswith("run_1:")
    assert live_contexts[0]["attempt_id"] != (
        f"run_1:{live_contexts[0]['started_at']}"
    )
    assert "source" not in live_contexts[0]
    assert "confidence" not in live_contexts[0]


@pytest.mark.asyncio
async def test_control_single_job_start_aborts_when_attempt_claim_is_rejected(
    monkeypatch,
):
    async def is_attempt_claimed(*_args, **_kwargs):
        return False

    def fail_if_target_is_loaded(_name):
        raise AssertionError("a rejected attempt must not load its target")

    def fail_if_heartbeat_starts(*_args, **_kwargs):
        raise AssertionError("a rejected attempt must not start a heartbeat")

    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_attempt_claimed)
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        fail_if_target_is_loaded,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_live_progress_heartbeat",
        fail_if_heartbeat_starts,
    )

    job_outcome = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_rejected",
            "target_module": "fake.module",
            "target_function": "process_data",
        },
    )

    assert job_outcome == {
        "status": "skipped",
        "run_id": "run_rejected",
        "reason": "newer_attempt_active",
    }
    assert current_live_progress_context() == {}


@pytest.mark.asyncio
async def test_live_progress_heartbeat_fails_closed_on_database_error(
    monkeypatch,
):
    sleep_counts = [0]
    live_progress_writes = []

    async def run_one_heartbeat_then_cancel(_interval):
        sleep_counts[0] += 1
        if sleep_counts[0] > 1:
            raise asyncio.CancelledError

    async def fail_heartbeat_persistence(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        control_lifecycle.asyncio,
        "sleep",
        run_one_heartbeat_then_cancel,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_persist_control_run_heartbeat",
        fail_heartbeat_persistence,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **payload: live_progress_writes.append(payload),
    )

    with pytest.raises(asyncio.CancelledError):
        await control_lifecycle._live_progress_heartbeat(
            "run_heartbeat",
            "ptg",
            "ptg_control_start",
            "2026-07-23T12:00:00.000000+00:00",
            attempt_id="attempt-a",
            attempt_started_at="2026-07-23T12:00:00.000000+00:00",
        )

    assert live_progress_writes == []


@pytest.mark.asyncio
async def test_control_single_job_start_ignores_arq_metadata_kwargs(monkeypatch):
    marks = []
    calls = []

    async def is_control_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_control_run_marked,
    )
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    run_response = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
        _max_tries=1,
    )

    assert run_response["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [mark_entry[1]["status"] for mark_entry in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_control_single_job_start_can_run_module_shutdown(monkeypatch):
    """Verify control single job start can run module shutdown."""
    marks = []
    calls = []

    async def is_control_run_marked(run_id, **kwargs):
        marks.append((run_id, kwargs))
        return True

    async def fake_target(ctx, task):
        calls.append(("target", dict(ctx.get("context") or {}), task))
        ctx.setdefault("context", {})["run"] = 1
        return None

    async def fake_shutdown(ctx):
        calls.append(("shutdown", dict(ctx.get("context") or {}), None))
        ctx.setdefault("context", {}).update(_SHUTDOWN_METRICS_BY_NAME)

    class FakeModule:
        process_data = staticmethod(fake_target)
        shutdown = staticmethod(fake_shutdown)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_control_run_marked)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    run_response = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "run_shutdown": True,
        },
    )

    assert run_response["status"] == "succeeded"
    assert calls == [
        ("target", {"control_run_id": "run_1"}, {"run_id": "run_1"}),
        ("shutdown", {"control_run_id": "run_1", "run": 1}, None),
    ]
    assert run_response["run_id"] == "run_1"
    assert [mark_entry[1]["status"] for mark_entry in marks] == ["running", "succeeded"]
    _assert_shutdown_terminal_metrics(marks[-1][1])


@pytest.mark.asyncio
async def test_control_single_job_start_isolates_concurrent_job_contexts(monkeypatch):
    """Keep per-run state isolated when control jobs execute concurrently."""
    terminal_marks_by_run_id = {}
    both_started = asyncio.Event()
    started_run_ids = set()
    worker_context_map = {"redis": object(), "context": {"worker_value": "shared"}}

    async def is_control_run_marked(run_id, **kwargs):
        if kwargs["status"] == "succeeded":
            terminal_marks_by_run_id[run_id] = kwargs
        return True

    async def fake_target(ctx, task):
        run_id = task["run_id"]
        assert ctx is not worker_context_map
        assert ctx["context"] is not worker_context_map["context"]
        ctx["context"]["audit"] = {"run_id": run_id}
        started_run_ids.add(run_id)
        if len(started_run_ids) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)
        return {"run_id": ctx["context"]["audit"]["run_id"]}

    async def fake_shutdown(_ctx):
        return None

    class FakeModule:
        process_data = staticmethod(fake_target)
        shutdown = staticmethod(fake_shutdown)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_control_run_marked)
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        lambda name: FakeModule if name == "fake.module" else None,
    )

    job_outcomes = await asyncio.gather(
        *(
            control_single_job_start(
                worker_context_map,
                {
                    "run_id": run_id,
                    "target_module": "fake.module",
                    "target_function": "process_data",
                    "run_shutdown": True,
                },
            )
            for run_id in ("run_1", "run_2")
        )
    )

    assert [outcome["result"]["run_id"] for outcome in job_outcomes] == ["run_1", "run_2"]
    assert terminal_marks_by_run_id["run_1"]["metrics"]["run_id"] == "run_1"
    assert terminal_marks_by_run_id["run_2"]["metrics"]["run_id"] == "run_2"
    assert worker_context_map == {
        "redis": worker_context_map["redis"],
        "context": {"worker_value": "shared"},
    }
