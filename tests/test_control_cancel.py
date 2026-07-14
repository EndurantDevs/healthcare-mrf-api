# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime as dt

import pytest

from process.control_cancel import ImportCancelledError, raise_if_cancelled, run_id_from_task
from process import control_lifecycle
from process.control_lifecycle import control_single_job_start


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

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    def capture_live_progress_context(**payload):
        live_contexts.append(payload)
        return real_set_live_progress_context(**payload)

    monkeypatch.setattr(control_lifecycle, "set_live_progress_context", capture_live_progress_context)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    result = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
    )

    assert result["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [item[1]["status"] for item in marks] == ["running", "succeeded"]
    assert live_contexts == [
        {
            "run_id": "run_1",
            "importer": "process_data",
            "status": "running",
            "started_at": live_contexts[0]["started_at"],
        }
    ]
    assert "source" not in live_contexts[0]
    assert "confidence" not in live_contexts[0]


@pytest.mark.asyncio
async def test_control_single_job_start_ignores_arq_metadata_kwargs(monkeypatch):
    marks = []
    calls = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(ctx, task):
        calls.append((ctx, task))
        return {"ok": True}

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    result = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "task": {"test_mode": True},
        },
        _max_tries=1,
    )

    assert result["status"] == "succeeded"
    assert calls[0][1] == {"test_mode": True, "run_id": "run_1"}
    assert [item[1]["status"] for item in marks] == ["running", "succeeded"]


@pytest.mark.asyncio
async def test_control_single_job_start_can_run_module_shutdown(monkeypatch):
    """Verify control single job start can run module shutdown."""
    marks = []
    calls = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(ctx, task):
        calls.append(("target", dict(ctx.get("context") or {}), task))
        ctx.setdefault("context", {})["run"] = 1
        return None

    async def fake_shutdown(ctx):
        calls.append(("shutdown", dict(ctx.get("context") or {}), None))
        ctx.setdefault("context", {}).update(
            {
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
                "stage_index_timings": [
                    {"index": "primary_npi", "seconds": 1.2},
                ],
                "preserve_control_run_finished_at": True,
            }
        )

    class FakeModule:
        process_data = staticmethod(fake_target)
        shutdown = staticmethod(fake_shutdown)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda name: FakeModule if name == "fake.module" else None)

    result = await control_single_job_start(
        {"redis": object()},
        {
            "run_id": "run_1",
            "target_module": "fake.module",
            "target_function": "process_data",
            "run_shutdown": True,
        },
    )

    assert result["status"] == "succeeded"
    assert calls == [
        ("target", {"control_run_id": "run_1"}, {"run_id": "run_1"}),
        ("shutdown", {"control_run_id": "run_1", "run": 1}, None),
    ]
    assert result["run_id"] == "run_1"
    assert [item[1]["status"] for item in marks] == ["running", "succeeded"]
    terminal = marks[-1][1]
    assert terminal["metrics"]["rows"] == 123
    assert terminal["metrics"]["staged_rows"] == 123
    assert terminal["metrics"]["source_table_shards"] == 64
    assert terminal["metrics"]["stage_index_profile"] == "serving"
    assert terminal["metrics"]["post_publish_index_profile"] == "serving"
    assert terminal["metrics"]["post_publish_index_concurrently"] is True
    assert terminal["metrics"]["post_publish_index_pending"] is False
    assert terminal["metrics"]["post_publish_index_total"] == 2
    assert terminal["metrics"]["post_publish_index_completed"] == 2
    assert terminal["metrics"]["post_publish_index_timings"][0]["index"] == "geo_bbox"
    assert terminal["metrics"]["post_publish_index_timings"][1]["index"] == "geo_idx"
    assert terminal["metrics"]["post_publish_skipped_indexes"] == []
    assert terminal["metrics"]["published_elapsed_seconds"] == 233.2
    assert terminal["metrics"]["phase_timings"]["entity-address-unified indexing stage"]["wall_seconds"] == 24.1
    assert terminal["metrics"]["stage_index_timings"][0]["index"] == "primary_npi"
    assert terminal["progress"]["done"] == 123
    assert terminal["preserve_finished_at"] is True


@pytest.mark.asyncio
async def test_control_single_job_start_isolates_concurrent_job_contexts(monkeypatch):
    terminal_marks_by_run_id = {}
    both_started = asyncio.Event()
    started_run_ids = set()
    worker_context_map = {"redis": object(), "context": {"worker_value": "shared"}}

    async def fake_mark(run_id, **kwargs):
        if kwargs["status"] == "succeeded":
            terminal_marks_by_run_id[run_id] = kwargs

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

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
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


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled(monkeypatch):
    marks = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(_ctx, _task):
        raise ImportCancelledError("cancelled")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    result = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert result["status"] == "canceled"
    assert [item[1]["status"] for item in marks] == ["running", "canceled"]


@pytest.mark.asyncio
async def test_control_single_job_start_marks_cancelled_task_failed(monkeypatch):
    marks = []

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_target(_ctx, _task):
        raise asyncio.CancelledError()

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    result = await control_single_job_start(
        {},
        {"run_id": "run_1", "target_module": "fake.module", "target_function": "process_data"},
    )

    assert result["status"] == "failed"
    assert [item[1]["status"] for item in marks] == ["running", "failed"]
    assert marks[-1][1]["error"]["code"] == "import_interrupted"


@pytest.mark.asyncio
async def test_control_run_update_uses_base_database_then_restores_override(monkeypatch):
    calls = []

    class FakeDb:
        _database_override = "healthporta_test"

        async def connect(self):
            calls.append(("connect", self._database_override))

        async def execute(self, stmt):
            calls.append(("execute", stmt, self._database_override))

    fake_db = FakeDb()
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "healthporta")
    monkeypatch.setattr(control_lifecycle, "db", fake_db)

    await control_lifecycle._execute_control_run_update("UPDATE")

    assert fake_db._database_override == "healthporta_test"
    assert calls == [
        ("connect", "healthporta"),
        ("execute", "UPDATE", "healthporta"),
        ("connect", "healthporta_test"),
    ]


@pytest.mark.asyncio
async def test_mark_control_run_throttles_repeated_running_db_update(monkeypatch):
    db_updates = []
    live_events = []
    status_events = []

    async def fake_update(stmt):
        db_updates.append(stmt)

    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "60")
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", lambda _key, _seconds: False)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", lambda **payload: live_events.append(payload))
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", lambda payload: status_events.append(payload))

    await control_lifecycle.mark_control_run(
        "run_1",
        status="running",
        phase_detail="mrf provider jobs running",
        progress_message="processed provider file",
        metrics={"last_provider_records": 123},
    )

    assert db_updates == []
    assert live_events[-1]["run_id"] == "run_1"
    assert live_events[-1]["status"] == "running"
    assert status_events[-1]["phase_detail"] == "mrf provider jobs running"


def test_control_run_heartbeat_update_values_prefers_live_progress():
    now = dt.datetime(2026, 6, 21, 12, 0, 0)
    values = control_lifecycle._control_run_heartbeat_update_values(
        "process_data",
        {
            "phase": "compact-serving scanner",
            "unit": "compressed_bytes",
            "done": 1048576,
            "total": 2097152,
            "pct": 50,
            "message": "compact-serving scanner 50.00%",
            "updated_at": "2026-06-21T12:00:00Z",
        },
        now,
    )

    assert values["status"] == "running"
    assert values["phase_detail"] == "compact-serving scanner"
    assert values["heartbeat_at"] == now
    assert values["finished_at"] is None
    assert values["progress"] == {
        "unit": "compressed_bytes",
        "done": 1048576,
        "total": 2097152,
        "pct": 50,
        "message": "compact-serving scanner 50.00%",
        "phase": "compact-serving scanner",
        "updated_at": "2026-06-21T12:00:00Z",
    }


def test_control_run_heartbeat_update_values_preserves_live_progress_detail():
    now = dt.datetime(2026, 6, 29, 14, 0, 0)
    detail = {
        "active_source_groups": [
            {
                "sample_source_id": "source_a",
                "sample_org_name": "Cigna",
                "current_resource": "PractitionerRole",
            }
        ]
    }

    values = control_lifecycle._control_run_heartbeat_update_values(
        "process_data",
        {
            "phase": "provider-directory importing resources",
            "unit": "steps",
            "done": 8,
            "total": 25,
            "pct": 32,
            "message": "imported resources for 8/25 source group(s)",
            "detail": detail,
            "updated_at": "2026-06-29T14:00:00Z",
        },
        now,
    )

    assert values["progress"]["detail"] == detail


@pytest.mark.asyncio
async def test_mark_control_run_always_persists_terminal_update(monkeypatch):
    db_updates = []

    async def fake_update(stmt):
        db_updates.append(stmt)

    def fail_slot(_key, _seconds):
        raise AssertionError("terminal updates must not consult the running throttle")

    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "60")
    monkeypatch.setattr(control_lifecycle, "_claim_control_run_db_update_slot", fail_slot)
    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", lambda **_payload: None)
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", lambda _payload: None)

    await control_lifecycle.mark_control_run(
        "run_1",
        status="succeeded",
        phase_detail="mrf import published",
        progress_message="succeeded",
    )

    assert len(db_updates) == 1


@pytest.mark.asyncio
async def test_mark_control_run_can_preserve_existing_finished_at(monkeypatch):
    db_updates = []
    live_events = []
    status_events = []

    async def fake_update(stmt):
        db_updates.append(stmt)

    monkeypatch.setattr(control_lifecycle, "_execute_control_run_update", fake_update)
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", lambda **payload: live_events.append(payload))
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", lambda payload: status_events.append(payload))

    await control_lifecycle.mark_control_run(
        "run_1",
        status="succeeded",
        phase_detail="entity-address-unified post-publish indexes warmed",
        progress_message="post-publish indexes warmed",
        preserve_finished_at=True,
    )

    values = {getattr(key, "key", str(key)): value for key, value in db_updates[0]._values.items()}
    assert "finished_at" not in values
    assert live_events[-1]["finished_at"] is None
    assert status_events[-1]["finished_at"] is None
