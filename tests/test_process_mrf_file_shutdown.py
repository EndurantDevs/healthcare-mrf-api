# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

from asyncpg.exceptions import DeadlockDetectedError
import pytest
from sqlalchemy import BigInteger, column

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")


class _ImportHistoryUpsert:
    def values(self, **_kwargs):
        return self

    def on_conflict_do_update(self, **_kwargs):
        return self

    async def status(self):
        return None


def _mrf_shutdown_stage(cls, suffix, schema_override=None):
    """Build the minimum dynamic-stage shape needed by shutdown."""
    return SimpleNamespace(
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __main_table__=getattr(cls, "__main_table__", cls.__tablename__),
        __my_initial_indexes__=list(getattr(cls, "__my_initial_indexes__", []) or []),
        __my_additional_indexes__=list(getattr(cls, "__my_additional_indexes__", []) or []),
        plan_id=column("plan_id") if cls is process_initial.Plan else None,
    )


@asynccontextmanager
async def _mrf_shutdown_transaction():
    """Provide the transaction boundary used by the mocked publication swap."""
    yield


def _install_mrf_shutdown_finalizer_mocks(monkeypatch, operations, mark_run, create_indexes):
    """Install focused finalizer dependencies while retaining operation order."""
    monkeypatch.setattr(process_initial, "mark_control_run", mark_run)
    monkeypatch.setattr(process_initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_initial, "get_import_schema", lambda *_args: "mrf")
    monkeypatch.setattr(process_initial, "make_class", _mrf_shutdown_stage)
    monkeypatch.setattr(process_initial, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        process_initial.db,
        "status",
        AsyncMock(side_effect=lambda statement, **_kwargs: operations.append(("sql", statement))),
    )
    monkeypatch.setattr(process_initial.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(process_initial.db, "transaction", _mrf_shutdown_transaction)
    monkeypatch.setattr(process_initial.db, "insert", lambda _table: _ImportHistoryUpsert())
    monkeypatch.setattr(process_initial, "_refresh_all_plan_drug_statistics", AsyncMock())
    monkeypatch.setattr(
        process_initial,
        "_refresh_mrf_address_summary",
        AsyncMock(
            side_effect=lambda _suffix, _schema, *, rebuild_indexes=True: operations.append(
                ("summary", rebuild_indexes)
            )
        ),
    )
    monkeypatch.setattr(process_initial, "_create_named_indexes", create_indexes)
    monkeypatch.setattr(process_initial, "source_enabled", lambda _source: False)
    monkeypatch.setattr(
        process_initial,
        "_plan_summary_dependencies_ready",
        AsyncMock(return_value=(False, ["mrf.plan_attributes"])),
    )

@pytest.mark.asyncio
async def test_process_json_index_marks_terminal_parse_error_done(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.sets = {}

        async def expire(self, *_args, **_kwargs):
            is_expiration_set = True
            return is_expiration_set

        async def hdel(self, _key, _field):
            return 0

        async def sadd(self, key, value):
            values = self.sets.setdefault(key, set())
            before = len(values)
            values.add(value)
            return 1 if len(values) > before else 0

    async def fake_download(_url, filename, **_kwargs):
        Path(filename).write_text("<html>not json</html>", encoding="utf-8")

    monkeypatch.setattr(process_initial, "download_it_and_save", fake_download)
    monkeypatch.setattr(process_initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(process_initial, "log_error", AsyncMock())

    redis = FakeRedis()
    context_by_field = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_terminal",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        context_by_field,
        {
            "url": "https://example.test/bad-index.json",
            "issuer_array": [11111],
            "context": context_by_field["context"],
        },
    )

    assert process_initial._mrf_url_job_id(
        "index",
        "run_test_terminal",
        "https://example.test/bad-index.json",
    ) in redis.sets[process_initial._mrf_state_key("run_test_terminal", "done_work")]


def test_split_json_array_file_to_chunks(tmp_path, monkeypatch):
    source = tmp_path / "providers.json"
    source.write_text(
        process_initial.json.dumps(
            [
                {"id": 1, "name": "alpha"},
                {"id": 2, "name": "bravo"},
                {"id": 3, "name": "charlie"},
            ]
        ),
        encoding="utf-8",
    )

    chunks = process_initial._split_json_array_file_to_chunks(
        str(source),
        tmp_path / "chunks",
        "provider",
        target_bytes=35,
    )

    assert len(chunks) > 1
    ids = []
    for chunk in chunks:
        payload = process_initial.json.loads(Path(chunk["path"]).read_text(encoding="utf-8"))
        ids.extend(item["id"] for item in payload)
    assert ids == [1, 2, 3]


@pytest.mark.asyncio
async def test_download_it_and_save_supports_file_urls(tmp_path):
    source = tmp_path / "source.json"
    target = tmp_path / "target.json"
    source.write_text("[{\"ok\": true}]", encoding="utf-8")

    await utils_module.download_it_and_save(source.absolute().as_uri(), str(target))

    assert target.read_text(encoding="utf-8") == "[{\"ok\": true}]"


@pytest.mark.asyncio
async def test_mrf_shutdown_requeues_while_parser_jobs_run(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.enqueued = []

        async def get(self, key):
            if key.endswith(":total_work"):
                return b"3"
            return None

        async def scard(self, key):
            assert key.endswith(":done_work")
            return 2

        async def enqueue_job(self, *args, **kwargs):
            self.enqueued.append((args, kwargs))

    mark_run = AsyncMock()
    monkeypatch.setattr(process_initial, "mark_control_run", mark_run)
    monkeypatch.setattr(
        process_initial,
        "_recover_missing_mrf_work",
        AsyncMock(
            return_value={
                "missing": 1,
                "recovered": 1,
                "active": 0,
                "unrecoverable": [],
                "exhausted": [],
            }
        ),
    )

    context_by_field = {
        "redis": FakeRedis(),
        "context": {
            "import_date": "20260618",
            "control_run_id": "run_mrf_wait",
            "test_mode": True,
        },
    }

    outcome_by_field = await process_initial.shutdown(context_by_field, {"context": context_by_field["context"], "test_mode": True})

    assert outcome_by_field == 1
    assert context_by_field["redis"].enqueued
    args, kwargs = context_by_field["redis"].enqueued[0]
    assert args[0] == "shutdown"
    assert args[1]["mrf_finalize_waits"] == 1
    assert kwargs["_queue_name"] == process_initial.MRF_FINISH_QUEUE_NAME
    assert kwargs["_defer_by"] == 60
    mark_run.assert_awaited_once()
    assert mark_run.await_args.kwargs["phase_detail"] == "mrf parser recovery queued"


@pytest.mark.asyncio
async def test_mrf_shutdown_fails_after_unrecoverable_work_wait(monkeypatch):
    class FakeRedis:
        async def get(self, key):
            if key.endswith(":total_work"):
                return b"3"
            return None

        async def scard(self, _key):
            return 2

    recovery_by_state = {
        "missing": 1,
        "recovered": 0,
        "active": 0,
        "unrecoverable": ["missing-work"],
        "exhausted": [],
    }
    mark_run = AsyncMock()
    monkeypatch.setattr(process_initial, "mark_control_run", mark_run)
    monkeypatch.setattr(
        process_initial,
        "_recover_missing_mrf_work",
        AsyncMock(return_value=recovery_by_state),
    )
    context_dict = {
        "redis": FakeRedis(),
        "context": {
            "import_date": "20260618",
            "control_run_id": "run_mrf_failed",
            "test_mode": True,
        },
    }

    with pytest.raises(RuntimeError, match="MRF parser recovery failed"):
        await process_initial.shutdown(
            context_dict,
            {"context": context_dict["context"], "test_mode": True, "mrf_finalize_waits": 5},
        )

    mark_run.assert_awaited_once()
    assert mark_run.await_args.kwargs["status"] == "failed"
    assert mark_run.await_args.kwargs["error"]["code"] == "mrf_parser_recovery_failed"


@pytest.mark.asyncio
async def test_mrf_shutdown_cleans_stale_finalize_jobs_when_already_finalized(monkeypatch):
    """Verify mrf shutdown cleans stale finalize jobs when already finalized."""
    class FakeRedis:
        def __init__(self):
            self.zrem_calls = []
            self.delete_calls = []

        async def get(self, key):
            if key.endswith(":finalized"):
                return b"1"
            return None
        async def zrange(self, *args):
            assert args == (process_initial.MRF_FINISH_QUEUE_NAME, 0, -1)
            return [
                b"shutdown_mrf_20260626",
                b"shutdown_mrf_20260626_wait_12",
                b"shutdown_mrf_20260626_lock_wait_13",
                b"shutdown_mrf_20260625_wait_4",
            ]
        async def zrem(self, *args):
            self.zrem_calls.append(args)
            return 1
        async def delete(self, *args):
            self.delete_calls.append(args)
            return len(args)
    mark_run = AsyncMock()
    monkeypatch.setattr(process_initial, "mark_control_run", mark_run)
    context_by_field = {
        "redis": FakeRedis(),
        "context": {
            "import_date": "20260626",
            "control_run_id": "run_mrf_done",
            "test_mode": True,
        },
    }
    outcome_by_field = await process_initial.shutdown(context_by_field, {"context": context_by_field["context"], "test_mode": True})
    assert outcome_by_field == 1
    assert set(context_by_field["redis"].zrem_calls) == {
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626"),
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626_wait_12"),
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626_lock_wait_13"),
    }
    assert context_by_field["redis"].delete_calls == [
        (
            "arq:job:shutdown_mrf_20260626",
            "arq:result:shutdown_mrf_20260626",
            "arq:retry:shutdown_mrf_20260626",
        ),
        (
            "arq:job:shutdown_mrf_20260626_lock_wait_13",
            "arq:result:shutdown_mrf_20260626_lock_wait_13",
            "arq:retry:shutdown_mrf_20260626_lock_wait_13",
        ),
        (
            "arq:job:shutdown_mrf_20260626_wait_12",
            "arq:result:shutdown_mrf_20260626_wait_12",
            "arq:retry:shutdown_mrf_20260626_wait_12",
        ),
    ]
    mark_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_mrf_shutdown_defers_serving_indexes_and_reports_phase_timings(monkeypatch):
    """Shutdown builds serving indexes after summary work and reports timings."""
    operations = []
    mark_run = AsyncMock()
    create_indexes = AsyncMock(
        side_effect=lambda stage, _schema, **kwargs: operations.append(
            ("indexes", stage.__tablename__, kwargs.get("names"))
        )
    )

    _install_mrf_shutdown_finalizer_mocks(
        monkeypatch,
        operations,
        mark_run,
        create_indexes,
    )

    context_by_field = {
        "import_date": "20260829",
        "control_run_id": "mrf-speedup-contract",
        "run": 1,
        "test_mode": True,
    }
    await process_initial.shutdown(
        {"context": context_by_field},
        {"context": context_by_field},
    )

    assert ("summary", False) in operations
    index_operations = [operation for operation in operations if operation[0] == "indexes"]
    assert index_operations == [
        ("indexes", "mrf_address_20260829", {"address_key"}),
        (
            "indexes",
            "mrf_address_evidence_20260829",
            {"npi_type_checksum", "address_key"},
        ),
        ("indexes", "mrf_address_20260829", None),
        ("indexes", "mrf_address_evidence_20260829", None),
    ]
    terminal_metrics = mark_run.await_args.kwargs["metrics"]
    assert terminal_metrics["plans_count"] == 1
    assert terminal_metrics["summary_rows"] == 0
    phase_names = [
        timing["phase"]
        for timing in terminal_metrics["mrf_finalize_phase_timings"]
    ]
    assert phase_names[:4] == [
        "plan_drug_statistics",
        "mrf_address_summary",
        "mrf_address_summary_analyze",
        "mrf_address_fast_path_indexes",
    ]
    assert phase_names[-1] == "mrf_finalize_total"


@pytest.mark.asyncio
async def test_mrf_shutdown_does_not_publish_when_fast_path_index_build_fails(monkeypatch):
    """An index-build failure must leave the live generation untouched."""
    operations = []
    create_indexes = AsyncMock(side_effect=RuntimeError("index build failed"))
    _install_mrf_shutdown_finalizer_mocks(
        monkeypatch,
        operations,
        AsyncMock(),
        create_indexes,
    )
    context_by_field = {
        "import_date": "20260829",
        "control_run_id": "mrf-speedup-failure-contract",
        "run": 1,
        "test_mode": True,
    }

    with pytest.raises(RuntimeError, match="index build failed"):
        await process_initial.shutdown(
            {"context": context_by_field},
            {"context": context_by_field},
        )

    assert not any(
        operation[0] == "sql" and "ALTER TABLE" in operation[1]
        for operation in operations
    )
