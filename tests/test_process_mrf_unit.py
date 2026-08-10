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
from sqlalchemy import BigInteger

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")


class RecoveryRedis:
    def __init__(self, pending_key, attempts_key, work_id, work_payload):
        self.hashes = {
            pending_key: {work_id: work_payload},
            attempts_key: {},
        }
        self.work_id = work_id
        self.enqueued = []
        self.deleted = []

    async def sdiff(self, _expected_key, _done_key):
        return {self.work_id.encode()}

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def exists(self, *_keys):
        return 0

    async def zscore(self, _key, _member):
        return None

    async def delete(self, *keys):
        self.deleted.extend(keys)
        return len(keys)

    async def enqueue_job(self, *args, **kwargs):
        self.enqueued.append((args, kwargs))
        return SimpleNamespace()

    async def hincrby(self, key, field, amount):
        values = self.hashes.setdefault(key, {})
        values[field] = int(values.get(field, 0)) + amount
        return values[field]

    async def expire(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_mrf_main_enqueues_init_file(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))

    await process_initial.main()

    fake_pool.enqueue_job.assert_awaited_once_with("init_file", {"test_mode": False}, _queue_name="arq:MRF")


@pytest.mark.asyncio
async def test_mrf_main_enqueues_init_file_test_mode(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))

    await process_initial.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with("init_file", {"test_mode": True}, _queue_name="arq:MRF")


def test_mrf_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.MRF.functions]
    assert names == [
        "init_file",
        "save_mrf_data",
        "process_plan",
        "process_json_index",
        "process_provider",
        "process_formulary",
    ]
    assert process_pkg.MRF.on_startup.__name__ == "startup"
    assert process_pkg.MRF.on_shutdown.__name__ == "mrf_worker_shutdown"


def test_mrf_address_npi_uses_bigint():
    assert isinstance(process_initial.MRFAddress.__table__.c.npi.type, BigInteger)


def test_mrf_queue_read_limit_can_be_configured(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MAX_MRF_JOBS", "12")
    monkeypatch.setenv("HLTHPRT_MRF_QUEUE_READ_LIMIT", "128")
    monkeypatch.setenv("HLTHPRT_MRF_JOB_TIMEOUT", "21600")

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.MRF.max_jobs == 12
        assert reloaded.MRF.queue_read_limit == 128
        assert reloaded.MRF.job_timeout == 21600
    finally:
        monkeypatch.delenv("HLTHPRT_MAX_MRF_JOBS", raising=False)
        monkeypatch.delenv("HLTHPRT_MRF_QUEUE_READ_LIMIT", raising=False)
        monkeypatch.delenv("HLTHPRT_MRF_JOB_TIMEOUT", raising=False)
        importlib.reload(process_pkg)


def test_ptg_queue_read_limit_defaults_wide_enough_for_parallel_burst_workers(monkeypatch):
    monkeypatch.delenv("HLTHPRT_MAX_PTG_JOBS", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG_QUEUE_READ_LIMIT", raising=False)

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTG.max_jobs == 1
        assert reloaded.PTG.queue_read_limit == 16
    finally:
        importlib.reload(process_pkg)


def test_ptg_queue_read_limit_can_be_configured(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MAX_PTG_JOBS", "2")
    monkeypatch.setenv("HLTHPRT_PTG_QUEUE_READ_LIMIT", "64")

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTG.max_jobs == 2
        assert reloaded.PTG.queue_read_limit == 64
    finally:
        monkeypatch.delenv("HLTHPRT_MAX_PTG_JOBS", raising=False)
        monkeypatch.delenv("HLTHPRT_PTG_QUEUE_READ_LIMIT", raising=False)
        importlib.reload(process_pkg)


def test_ptg_lane_worker_defaults(monkeypatch):
    for name in (
        "HLTHPRT_MAX_PTG_SMALL_JOBS",
        "HLTHPRT_MAX_PTG_NORMAL_JOBS",
        "HLTHPRT_MAX_PTG_LARGE_JOBS",
        "HLTHPRT_MAX_PTG_HUGE_JOBS",
    ):
        monkeypatch.delenv(name, raising=False)

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTGSmall.queue_name == "arq:PTGSmall"
        assert reloaded.PTGSmall.max_jobs == 16
        assert reloaded.PTGNormal.max_jobs == 8
        assert reloaded.PTGLarge.max_jobs == 3
        assert reloaded.PTGHuge.max_jobs == 1
        assert reloaded.PTGHuge.queue_read_limit == 16
    finally:
        importlib.reload(process_pkg)


def test_mrf_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.MRF_finish.functions]
    assert names == ["shutdown"]
    assert process_pkg.MRF_finish.queue_name == "arq:MRF_finish"
    assert process_pkg.MRF_finish.max_jobs == 1
    assert process_pkg.MRF_finish.queue_read_limit == 1


def test_mrf_parallel_range_downloads_are_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PREFER_COMPRESSED_STREAM", raising=False)
    reloaded = importlib.reload(utils_module)

    assert reloaded.PREFER_COMPRESSED_STREAM is False


def test_mrf_compressed_stream_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PREFER_COMPRESSED_STREAM", "true")
    reloaded = importlib.reload(utils_module)

    assert reloaded.PREFER_COMPRESSED_STREAM is True

    monkeypatch.setenv("HLTHPRT_PREFER_COMPRESSED_STREAM", "false")
    importlib.reload(utils_module)


def test_mrf_flush_rows_are_configurable(monkeypatch):
    monkeypatch.delenv("HLTHPRT_MRF_PLAN_FLUSH_ROWS", raising=False)
    monkeypatch.delenv("HLTHPRT_SAVE_PER_PACK", raising=False)
    monkeypatch.delenv("HLTHPRT_MRF_PROVIDER_FLUSH_ROWS", raising=False)
    monkeypatch.delenv("HLTHPRT_MRF_FORMULARY_FLUSH_ROWS", raising=False)

    assert process_initial._mrf_plan_flush_rows() == 2000
    assert process_initial._mrf_provider_flush_rows() == 50000
    assert process_initial._mrf_formulary_flush_rows() == 50000

    monkeypatch.setenv("HLTHPRT_SAVE_PER_PACK", "123")
    assert process_initial._mrf_plan_flush_rows() == 123

    monkeypatch.setenv("HLTHPRT_MRF_PLAN_FLUSH_ROWS", "456")
    monkeypatch.setenv("HLTHPRT_MRF_PROVIDER_FLUSH_ROWS", "789")
    monkeypatch.setenv("HLTHPRT_MRF_FORMULARY_FLUSH_ROWS", "321")
    assert process_initial._mrf_plan_flush_rows() == 456
    assert process_initial._mrf_provider_flush_rows() == 789
    assert process_initial._mrf_formulary_flush_rows() == 321


@pytest.mark.asyncio
async def test_process_json_index_dedupes_provider_url_jobs(monkeypatch):
    """Verify process json index dedupes provider url jobs."""
    async def fake_download(_url, filename, **_kwargs):
        payload = {
            "plan_urls": [],
            "formulary_urls": [],
            "provider_urls": [
                "https://example.test/providers.json",
                "https://example.test/providers.json",
            ],
        }
        with open(filename, "w", encoding="utf-8") as fp:
            process_initial.json.dump(payload, fp)

    monkeypatch.setattr(process_initial, "download_it_and_save", fake_download)
    monkeypatch.setattr(process_initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda *_args, **_kwargs: SimpleNamespace())

    redis = _UniqueWorkRedis()
    context_by_field = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_123",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        context_by_field,
        {
            "url": "https://example.test/index.json",
            "issuer_array": [11111, 22222],
            "context": context_by_field["context"],
        },
    )

    provider_calls = [call for call in redis.calls if call[0][0] == "process_provider"]
    assert len(provider_calls) == 1
    args, kwargs = provider_calls[0]
    assert args[1]["url"] == "https://example.test/providers.json"
    assert args[1]["issuer_array"] == [11111, 22222]
    assert kwargs["_queue_name"] == process_initial.MRF_QUEUE_NAME
    assert kwargs["_job_id"] == process_initial._mrf_url_job_id(
        "provider",
        "run_test_123",
        "https://example.test/providers.json",
    )
    assert redis.values[process_initial._mrf_state_key("run_test_123", "total_work")] == 1
    assert process_initial._mrf_url_job_id(
        "index",
        "run_test_123",
        "https://example.test/index.json",
    ) in redis.sets[process_initial._mrf_state_key("run_test_123", "done_work")]


class _UniqueWorkRedis:
    def __init__(self):
        self.calls = []
        self.values = {}
        self.sets = {}
        self.hashes = {}

    async def enqueue_job(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return SimpleNamespace()

    async def incrby(self, key, value):
        self.values[key] = int(self.values.get(key, 0)) + int(value)

    async def expire(self, *_args, **_kwargs):
        is_expiration_set = True
        return is_expiration_set

    async def hsetnx(self, key, field, value):
        values = self.hashes.setdefault(key, {})
        if field in values:
            return 0
        values[field] = value
        return 1

    async def hdel(self, key, field):
        return int(self.hashes.get(key, {}).pop(field, None) is not None)

    async def sadd(self, key, value):
        values = self.sets.setdefault(key, set())
        before = len(values)
        values.add(value)
        return 1 if len(values) > before else 0


@pytest.mark.asyncio
async def test_process_json_index_test_limit_counts_unique_registered_jobs(monkeypatch):
    """Verify process json index test limit counts unique registered jobs."""

    async def fake_download(_url, filename, **_kwargs):
        payload = {
            "plan_urls": [
                "https://example.test/plan-a.json",
                "https://example.test/plan-a.json",
                "https://example.test/plan-b.json",
            ],
            "formulary_urls": [],
            "provider_urls": [],
        }
        with open(filename, "w", encoding="utf-8") as fp:
            process_initial.json.dump(payload, fp)

    monkeypatch.setattr(process_initial, "download_it_and_save", fake_download)
    monkeypatch.setattr(process_initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda *_args, **_kwargs: SimpleNamespace())

    redis = _UniqueWorkRedis()
    context_by_field = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_unique_limit",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        context_by_field,
        {
            "url": "https://example.test/index.json",
            "issuer_array": [11111],
            "context": context_by_field["context"],
        },
    )

    plan_calls = [call for call in redis.calls if call[0][0] == "process_plan"]
    assert [call[0][1]["url"] for call in plan_calls] == [
        "https://example.test/plan-a.json",
        "https://example.test/plan-b.json",
    ]
    assert redis.values[process_initial._mrf_state_key("run_test_unique_limit", "total_work")] == 2


@pytest.mark.asyncio
async def test_register_mrf_work_counts_unique_work_ids_once():
    class FakeRedis:
        def __init__(self):
            self.values = {}
            self.sets = {}

        async def incrby(self, key, value):
            self.values[key] = int(self.values.get(key, 0)) + int(value)

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

    redis = FakeRedis()

    assert await process_initial._has_registered_mrf_work(redis, "run_test", "work-1") is True
    assert await process_initial._has_registered_mrf_work(redis, "run_test", "work-1") is False
    assert await process_initial._has_registered_mrf_work(redis, "run_test", "work-2") is True

    assert redis.values[process_initial._mrf_state_key("run_test", "total_work")] == 2
    assert redis.sets[process_initial._mrf_state_key("run_test", "expected_work")] == {"work-1", "work-2"}


@pytest.mark.asyncio
async def test_recover_missing_mrf_work_requeues_persisted_payload():
    """A missing ARQ job is rebuilt from its durable run payload."""
    work_id = "mrf:provider:run_test:abc123"
    pending_key = process_initial._mrf_state_key("run_test", "pending_work")
    attempts_key = process_initial._mrf_state_key("run_test", "recovery_attempts")
    task_dict = {
        "url": "https://example.test/providers.json",
        "issuer_array": [12345],
        "context": {"control_run_id": "run_test", "import_date": "20260613"},
        "work_id": work_id,
    }
    work_payload = process_initial.serialize_job(
        {"function": "process_provider", "task": task_dict}
    )
    redis = RecoveryRedis(pending_key, attempts_key, work_id, work_payload)

    recovery_by_state = await process_initial._recover_missing_mrf_work(redis, "run_test")

    assert recovery_by_state == {
        "missing": 1,
        "recovered": 1,
        "active": 0,
        "unrecoverable": [],
        "exhausted": [],
    }
    assert redis.enqueued == [
        (
            ("process_provider", task_dict),
            {"_queue_name": process_initial.MRF_QUEUE_NAME, "_job_id": work_id},
        )
    ]
    assert redis.hashes[attempts_key][work_id] == 1
    assert redis.deleted == [f"arq:result:{work_id}", f"arq:retry:{work_id}"]


@pytest.mark.asyncio
async def test_mrf_worker_shutdown_marks_drained_queue_for_finalizer(monkeypatch):
    class FakeRedis:
        async def zcard(self, queue_name):
            assert queue_name == process_initial.MRF_QUEUE_NAME
            return 0

        async def get(self, key):
            assert key.endswith(":total_work")
            return b"12"

        async def scard(self, key):
            assert key.endswith(":done_work")
            return 11

    mark_run = AsyncMock()
    monkeypatch.setattr(process_initial, "mark_control_run", mark_run)
    worker_context_dict = {
        "redis": FakeRedis(),
        "context": {"control_run_id": "run_test", "import_date": "20260613"},
    }

    await process_initial.mrf_worker_shutdown(worker_context_dict)

    mark_run.assert_awaited_once()
    assert mark_run.await_args.kwargs["status"] == "running"
    assert "finalizing" in mark_run.await_args.kwargs["phase_detail"]
    assert mark_run.await_args.kwargs["progress"]["done"] == 11
