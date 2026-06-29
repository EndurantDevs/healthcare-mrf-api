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
    class FakeRedis:
        def __init__(self):
            self.calls = []
            self.values = {}
            self.sets = {}

        async def enqueue_job(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return SimpleNamespace()

        async def incrby(self, key, value):
            self.values[key] = int(self.values.get(key, 0)) + int(value)

        async def expire(self, *_args, **_kwargs):
            return True

        async def sadd(self, key, value):
            values = self.sets.setdefault(key, set())
            before = len(values)
            values.add(value)
            return 1 if len(values) > before else 0

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

    redis = FakeRedis()
    ctx = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_123",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        ctx,
        {
            "url": "https://example.test/index.json",
            "issuer_array": [11111, 22222],
            "context": ctx["context"],
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


@pytest.mark.asyncio
async def test_process_json_index_test_limit_counts_unique_registered_jobs(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.calls = []
            self.values = {}
            self.sets = {}

        async def enqueue_job(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return SimpleNamespace()

        async def incrby(self, key, value):
            self.values[key] = int(self.values.get(key, 0)) + int(value)

        async def expire(self, *_args, **_kwargs):
            return True

        async def sadd(self, key, value):
            values = self.sets.setdefault(key, set())
            before = len(values)
            values.add(value)
            return 1 if len(values) > before else 0

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

    redis = FakeRedis()
    ctx = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_unique_limit",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        ctx,
        {
            "url": "https://example.test/index.json",
            "issuer_array": [11111],
            "context": ctx["context"],
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
            return True

        async def sadd(self, key, value):
            values = self.sets.setdefault(key, set())
            before = len(values)
            values.add(value)
            return 1 if len(values) > before else 0

    redis = FakeRedis()

    assert await process_initial._register_mrf_work(redis, "run_test", "work-1") is True
    assert await process_initial._register_mrf_work(redis, "run_test", "work-1") is False
    assert await process_initial._register_mrf_work(redis, "run_test", "work-2") is True

    assert redis.values[process_initial._mrf_state_key("run_test", "total_work")] == 2
    assert redis.sets[process_initial._mrf_state_key("run_test", "expected_work")] == {"work-1", "work-2"}


@pytest.mark.asyncio
async def test_process_json_index_marks_terminal_parse_error_done(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.sets = {}

        async def expire(self, *_args, **_kwargs):
            return True

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
    ctx = {
        "redis": redis,
        "context": {
            "import_date": "20260613",
            "control_run_id": "run_test_terminal",
            "test_mode": True,
        },
    }

    await process_initial.process_json_index(
        ctx,
        {
            "url": "https://example.test/bad-index.json",
            "issuer_array": [11111],
            "context": ctx["context"],
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

    ctx = {
        "redis": FakeRedis(),
        "context": {
            "import_date": "20260618",
            "control_run_id": "run_mrf_wait",
            "test_mode": True,
        },
    }

    result = await process_initial.shutdown(ctx, {"context": ctx["context"], "test_mode": True})

    assert result == 1
    assert ctx["redis"].enqueued
    args, kwargs = ctx["redis"].enqueued[0]
    assert args[0] == "shutdown"
    assert args[1]["mrf_finalize_waits"] == 1
    assert kwargs["_queue_name"] == process_initial.MRF_FINISH_QUEUE_NAME
    assert kwargs["_defer_by"] == 60
    mark_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_mrf_shutdown_cleans_stale_finalize_jobs_when_already_finalized(monkeypatch):
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

    ctx = {
        "redis": FakeRedis(),
        "context": {
            "import_date": "20260626",
            "control_run_id": "run_mrf_done",
            "test_mode": True,
        },
    }

    result = await process_initial.shutdown(ctx, {"context": ctx["context"], "test_mode": True})

    assert result == 1
    assert set(ctx["redis"].zrem_calls) == {
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626"),
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626_wait_12"),
        (process_initial.MRF_FINISH_QUEUE_NAME, "shutdown_mrf_20260626_lock_wait_13"),
    }
    assert ctx["redis"].delete_calls == [
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


def test_transparency_zip_path_is_unique_per_source(tmp_path):
    first = process_initial._transparency_zip_path(str(tmp_path), 0, {"year": "2026"})
    second = process_initial._transparency_zip_path(str(tmp_path), 1, {"year": "2025"})

    assert first.endswith("transparency_0_2026.zip")
    assert second.endswith("transparency_1_2025.zip")
    assert first != second


def test_claims_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing.functions]
    assert names == ["claims_pricing_start", "claims_pricing_process_chunk"]
    assert process_pkg.ClaimsPricing.queue_name == "arq:ClaimsPricing"


def test_claims_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing_finish.functions]
    assert names == ["claims_pricing_finalize"]
    assert process_pkg.ClaimsPricing_finish.queue_name == "arq:ClaimsPricing_finish"
    assert process_pkg.ClaimsPricing_finish.max_tries == 720
    assert process_pkg.DrugClaims_finish.max_tries == 720


def test_job_serializer_handles_exceptions():
    encoded = process_pkg.MRF.job_serializer(RuntimeError("boom"))
    decoded = process_pkg.MRF.job_deserializer(encoded)
    assert decoded["__type__"] == "exception"
    assert decoded["name"] == "RuntimeError"
    assert decoded["message"] == "boom"


def test_plan_attributes_cli_accepts_test_flag(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_plan_attributes", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg, "_run", fake_run)

    process_pkg.plan_attributes.callback(test=True)

    fake_initiate.assert_called_once_with(test_mode=True)


def test_claims_pricing_cli_accepts_test_flag(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_claims_pricing", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg, "_run", fake_run)

    process_pkg.claims_pricing.callback(test=True, import_id="dev1")

    fake_initiate.assert_called_once_with(test_mode=True, import_id="dev1")

@pytest.mark.asyncio
async def test_mrf_startup_sets_utc_time(monkeypatch):
    monkeypatch.setattr(process_initial, "my_init_db", AsyncMock())
    monkeypatch.setattr(process_initial.db, "status", AsyncMock())
    monkeypatch.setattr(process_initial.db, "create_table", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda cls, suffix: SimpleNamespace(
        __main_table__=cls.__tablename__,
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
        __my_index_elements__=["id"]
    ))

    ctx = {}
    await process_initial.startup(ctx)

    delta = datetime.datetime.utcnow() - ctx["context"]["start"]
    assert delta.total_seconds() < 2


@pytest.mark.asyncio
async def test_finish_main_enqueues_shutdown(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))
    monkeypatch.setattr(process_initial.os, "environ", {})

    await process_initial.finish_main(test_mode=True, import_id="20260402")

    fake_pool.enqueue_job.assert_awaited_once_with(
        "shutdown",
        {"context": {"import_date": "20260402", "test_mode": True}, "test_mode": True},
        _queue_name="arq:MRF_finish",
        _job_id="shutdown_mrf_20260402",
    )


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_sets_local_work_mem_and_analyzes(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    def fake_make_class(cls, suffix, schema_override=None):
        return SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}")

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "2GB")
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert statements[0] == "SET LOCAL work_mem = '2GB';"
    assert statements[1] == "ANALYZE mrf.mrf_address_evidence_20260612;"
    assert "INSERT INTO mrf.mrf_address_20260612" in statements[2]
    assert "FROM mrf.mrf_address_evidence_20260612" in statements[2]
    assert "ON CONFLICT (npi, type, checksum) DO UPDATE" in statements[2]
    assert len(statements) == 3


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_defers_source_array_indexes(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    address_cls = SimpleNamespace(
        __tablename__="mrf_address_20260612",
        __my_additional_indexes__=[
            {"index_elements": ("type", "npi"), "name": "type_npi"},
            {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
            {"index_elements": ("source_issuer_ids",), "using": "gin", "name": "source_issuer_ids"},
            {"index_elements": ("source_issuer_names",), "using": "gin", "name": "source_issuer_names"},
        ],
    )
    evidence_cls = SimpleNamespace(__tablename__="mrf_address_evidence_20260612")

    def fake_make_class(cls, suffix, schema_override=None):
        if cls is process_initial.MRFAddress:
            return address_cls
        return evidence_cls

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", "1")
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_DEFER_SOURCE_INDEXES", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    upsert_index = next(i for i, statement in enumerate(statements) if "INSERT INTO mrf.mrf_address_20260612" in statement)
    assert statements[1:4] == [
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_address_sources;",
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_source_issuer_ids;",
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_source_issuer_names;",
    ]
    assert upsert_index == 5
    assert statements[6:9] == [
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_address_sources ON mrf.mrf_address_20260612 USING gin (address_sources);",
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_source_issuer_ids ON mrf.mrf_address_20260612 USING gin (source_issuer_ids);",
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_source_issuer_names ON mrf.mrf_address_20260612 USING gin (source_issuer_names);",
    ]
    assert statements[9] == "ANALYZE mrf.mrf_address_20260612;"
    assert all("type_npi" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_defers_all_address_indexes_when_ingest_skips_aggregate(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    address_cls = SimpleNamespace(
        __tablename__="mrf_address_20260612",
        __my_initial_indexes__=[{"index_elements": ("checksum",)}],
        __my_additional_indexes__=[
            {"index_elements": ("type", "npi"), "name": "type_npi"},
            {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
        ],
    )
    evidence_cls = SimpleNamespace(__tablename__="mrf_address_evidence_20260612")

    def fake_make_class(cls, suffix, schema_override=None):
        if cls is process_initial.MRFAddress:
            return address_cls
        return evidence_cls

    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_DEFER_SOURCE_INDEXES", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_checksum;" in statements
    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_type_npi;" in statements
    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_address_sources;" in statements
    upsert_index = next(i for i, statement in enumerate(statements) if "INSERT INTO mrf.mrf_address_20260612" in statement)
    recreate_index = next(i for i, statement in enumerate(statements) if "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_checksum" in statement)
    assert upsert_index < recreate_index


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_accepts_statement_timeout(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", "45min")
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(
        process_initial,
        "make_class",
        lambda cls, suffix, schema_override=None: SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}"),
    )

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert statements[0] == "SET LOCAL work_mem = '1GB';"
    assert statements[1] == "SET LOCAL statement_timeout = '45min';"
    assert statements[2] == "ANALYZE mrf.mrf_address_evidence_20260612;"
    assert "INSERT INTO mrf.mrf_address_20260612" in statements[3]


def test_postgres_setting_value_rejects_unsafe_env(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "1GB'; DROP TABLE mrf.plan; --")

    with pytest.raises(ValueError, match="HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM"):
        process_initial._postgres_setting_value("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "1GB")


@pytest.mark.asyncio
async def test_plan_summary_dependencies_ready(monkeypatch):
    values = {
        "mrf.plan_attributes": "mrf.plan_attributes",
        "mrf.plan_benefits": "mrf.plan_benefits",
        "mrf.plan_prices": None,
    }

    async def fake_scalar(stmt, **params):
        assert "to_regclass" in stmt
        return values[params["qualified_name"]]

    monkeypatch.setattr(process_initial.db, "scalar", fake_scalar)

    ready, missing = await process_initial._plan_summary_dependencies_ready("mrf")

    assert ready is False
    assert missing == ["plan_prices"]


@pytest.mark.asyncio
async def test_refresh_plan_drug_statistics_upserts_concurrent_refreshes(monkeypatch):
    from sqlalchemy import Boolean, Column, DateTime, Integer, MetaData, String, Table

    metadata = MetaData()
    plan_drug = Table(
        "plan_drug_raw_test",
        metadata,
        Column("plan_id", String),
        Column("drug_tier", String),
        Column("prior_authorization", Boolean),
        Column("step_therapy", Boolean),
        Column("quantity_limit", Boolean),
        Column("last_updated_on", DateTime),
    )
    stats = Table(
        "plan_drug_stats_test",
        metadata,
        Column("plan_id", String, primary_key=True),
        Column("total_drugs", Integer),
        Column("auth_required", Integer),
        Column("auth_not_required", Integer),
        Column("step_required", Integer),
        Column("step_not_required", Integer),
        Column("quantity_limit", Integer),
        Column("quantity_no_limit", Integer),
        Column("last_updated_on", DateTime),
    )
    tier_stats = Table(
        "plan_drug_tier_stats_test",
        metadata,
        Column("plan_id", String, primary_key=True),
        Column("drug_tier", String, primary_key=True),
        Column("drug_count", Integer),
    )
    inserts = []

    class FakeInsert:
        def __init__(self, table):
            self.table = table
            self.excluded = SimpleNamespace(
                **{column.name: f"excluded_{column.name}" for column in table.c}
            )

        def from_select(self, columns, select_stmt):
            self.columns = columns
            self.select_stmt = select_stmt
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            self.index_elements = index_elements
            self.set_ = set_
            return self

        async def status(self):
            inserts.append(self)

    def fake_make_class(cls, suffix, schema_override=None):
        table_by_cls = {
            process_initial.PlanDrugRaw: plan_drug,
            process_initial.PlanDrugStats: stats,
            process_initial.PlanDrugTierStats: tier_stats,
        }
        return SimpleNamespace(__table__=table_by_cls[cls])

    def fail_delete(_table):
        raise AssertionError("aggregate refresh should upsert instead of delete then insert")

    monkeypatch.setattr(process_initial, "make_class", fake_make_class)
    monkeypatch.setattr(process_initial.db, "insert", lambda table: FakeInsert(table))
    monkeypatch.setattr(process_initial.db, "delete", fail_delete)

    await process_initial._refresh_plan_drug_statistics({"94529WI0240007"}, "20260612", "mrf")

    assert len(inserts) == 2
    stats_insert, tier_insert = inserts
    assert stats_insert.table is stats
    assert [column.name for column in stats_insert.index_elements] == ["plan_id"]
    assert stats_insert.set_ == {
        "total_drugs": "excluded_total_drugs",
        "auth_required": "excluded_auth_required",
        "auth_not_required": "excluded_auth_not_required",
        "step_required": "excluded_step_required",
        "step_not_required": "excluded_step_not_required",
        "quantity_limit": "excluded_quantity_limit",
        "quantity_no_limit": "excluded_quantity_no_limit",
        "last_updated_on": "excluded_last_updated_on",
    }
    assert tier_insert.table is tier_stats
    assert [column.name for column in tier_insert.index_elements] == ["plan_id", "drug_tier"]
    assert tier_insert.set_ == {"drug_count": "excluded_drug_count"}


@pytest.mark.asyncio
async def test_refresh_all_plan_drug_statistics_batches_from_stage(monkeypatch):
    calls = []

    async def fake_scalar(sql, **params):
        assert "to_regclass" in sql
        assert params["qualified_name"] == "mrf.plan_drug_raw_20260612"
        return "mrf.plan_drug_raw_20260612"

    async def fake_all(sql):
        calls.append(str(sql))
        return [SimpleNamespace(plan_id="94529WI0240007"), ("94529WI0240008",)]

    async def fake_refresh(plan_ids, import_date, db_schema):
        calls.append((set(plan_ids), import_date, db_schema))

    monkeypatch.setattr(
        process_initial,
        "make_class",
        lambda cls, suffix, schema_override=None: SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}"),
    )
    monkeypatch.setattr(process_initial.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_initial.db, "all", fake_all)
    monkeypatch.setattr(process_initial, "_refresh_plan_drug_statistics", fake_refresh)

    await process_initial._refresh_all_plan_drug_statistics("20260612", "mrf")

    assert "SELECT DISTINCT plan_id" in calls[0]
    assert calls[1] == ({"94529WI0240007", "94529WI0240008"}, "20260612", "mrf")


@pytest.mark.asyncio
async def test_refresh_do_business_as(monkeypatch):
    calls = []

    async def fake_scalar(sql):
        calls.append(sql)
        if "to_regclass" in str(sql):
            return "mrf.npi_other_identifier"
        return 0

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", AsyncMock())
    monkeypatch.setattr(process_npi.db, "scalar", fake_scalar)
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    result = await process_npi.refresh_do_business_as()

    sql_strings = [str(sql) for sql in calls]
    assert result == (0, 0)
    assert not any(s.strip().startswith("UPDATE mrf.npi") for s in sql_strings)
    assert any("IS DISTINCT FROM" in s for s in sql_strings)
    assert any("NOT EXISTS" in s for s in sql_strings)
    assert any("do_business_as_text = COALESCE" in str(sql) for sql in calls)


@pytest.mark.asyncio
async def test_refresh_do_business_as_skips_when_source_missing(monkeypatch):
    status_mock = AsyncMock()
    scalar_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", status_mock)
    monkeypatch.setattr(process_npi.db, "scalar", scalar_mock)
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    await process_npi.refresh_do_business_as(
        target_table="npi_20260214",
        source_table="npi_other_identifier_20260214",
    )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_taxonomy_arrays_uses_deterministic_changed_only_update(monkeypatch):
    calls = []

    async def fake_scalar(sql):
        calls.append(str(sql))
        return 12

    monkeypatch.setattr(process_npi.db, "scalar", fake_scalar)

    updated = await process_npi.refresh_taxonomy_arrays(
        address_table="npi_address_20260613",
        taxonomy_table="npi_taxonomy_20260613",
        schema="mrf",
    )

    assert updated == 12
    sql = calls[0]
    assert "ARRAY_AGG(DISTINCT nucc.int_code ORDER BY nucc.int_code)::int[]" in sql
    assert "addr.taxonomy_array IS DISTINCT FROM sub.res" in sql


@pytest.mark.asyncio
async def test_push_objects_rewrite_respects_parameter_limit(monkeypatch):
    status_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            return self

        async def status(self):
            status_calls.append(len(self.chunk))

    def _fake_insert(_table):
        return _FakeStmt()

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="fake_upsert_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    records = []
    for idx in range(8):
        row = {"id": idx, "value": f"v{idx}"}
        for extra in range(38):
            row[f"c{extra}"] = extra
        records.append(row)

    monkeypatch.setattr(utils_module.db, "insert", _fake_insert)
    monkeypatch.setenv("HLTHPRT_MAX_INSERT_PARAMETERS", "120")
    monkeypatch.setenv("HLTHPRT_DRIVER_PARAM_LIMIT", "32767")

    await utils_module.push_objects(records, fake_cls, rewrite=True, use_copy=False)

    assert status_calls == [3, 3, 2]


@pytest.mark.asyncio
async def test_push_objects_rewrite_prefers_copy_first(monkeypatch):
    copy_calls = []

    class _FakeDriver:
        async def copy_records_to_table(self, table_name, schema_name=None, columns=None, records=None):
            rows = []
            async for row in records:
                rows.append(row)
            copy_calls.append(
                {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "columns": list(columns or []),
                    "row_count": len(rows),
                }
            )

    class _FakeRaw:
        def __init__(self):
            self.driver_connection = _FakeDriver()

    class _FakeConn:
        def __init__(self):
            self.raw_connection = _FakeRaw()

    class _AcquireCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _fail_insert(*_args, **_kwargs):
        raise AssertionError("insert fallback should not run when COPY succeeds")

    fake_table = SimpleNamespace(schema="mrf", c=[])
    fake_cls = SimpleNamespace(
        __tablename__="copy_first_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )
    rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]

    monkeypatch.setattr(utils_module.db, "acquire", lambda: _AcquireCtx())
    monkeypatch.setattr(utils_module.db, "insert", _fail_insert)

    await utils_module.push_objects(rows, fake_cls, rewrite=True)

    assert len(copy_calls) == 1
    assert copy_calls[0]["table_name"] == "copy_first_table"
    assert copy_calls[0]["schema_name"] == "mrf"
    assert copy_calls[0]["columns"] == ["id", "value"]
    assert copy_calls[0]["row_count"] == 2


@pytest.mark.asyncio
async def test_push_objects_falls_back_when_copy_rejects_json_payload(monkeypatch):
    status_calls = []

    class _FakeDriver:
        async def copy_records_to_table(self, *_args, **_kwargs):
            raise TypeError("descriptor 'encode' for 'str' objects doesn't apply to a 'dict' object")

    class _FakeRaw:
        def __init__(self):
            self.driver_connection = _FakeDriver()

    class _FakeConn:
        def __init__(self):
            self.raw_connection = _FakeRaw()

    class _AcquireCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeStmt:
        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_nothing(self, index_elements=None):
            self.index_elements = index_elements
            return self

        async def status(self):
            status_calls.append(self.chunk)

    fake_table = SimpleNamespace(schema="mrf", c=[])
    fake_cls = SimpleNamespace(
        __tablename__="json_payload_table",
        __table__=fake_table,
        __my_index_elements__=["id"],
    )
    rows = [
        {"id": 1, "payload": {"telemedicine": True}},
        {"id": 2, "payload": {"telemedicine": False}},
    ]

    monkeypatch.setattr(utils_module.db, "acquire", lambda: _AcquireCtx())
    monkeypatch.setattr(utils_module.db, "insert", lambda _table: _FakeStmt())

    await utils_module.push_objects(rows, fake_cls)

    assert status_calls == [rows]


@pytest.mark.asyncio
async def test_push_objects_retries_deadlock_during_fallback_insert(monkeypatch):
    status_attempts = []
    sleep_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            self.index_elements = index_elements
            self.set_dict = set_
            return self

        async def status(self):
            status_attempts.append(len(self.chunk))
            if len(status_attempts) == 1:
                raise DeadlockDetectedError("deadlock detected")

    async def _sleep(delay):
        sleep_calls.append(delay)

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(schema="mrf", c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="deadlock_retry_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    monkeypatch.setattr(utils_module.db, "insert", lambda _table: _FakeStmt())
    monkeypatch.setattr(utils_module.asyncio, "sleep", _sleep)
    monkeypatch.setenv("HLTHPRT_DB_DEADLOCK_RETRIES", "2")

    await utils_module.push_objects([{"id": 1, "value": "a"}], fake_cls, rewrite=True, use_copy=False)

    assert status_attempts == [1, 1]
    assert sleep_calls == [0.5]


def test_parallel_download_disabled_host_matching(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "www22.elevancehealth.com,.blocked.example",
    )

    assert utils_module._parallel_download_disabled_for_url(
        "https://www22.elevancehealth.com/cms/PROVIDERS_TX_2_OF_2.json"
    )
    assert utils_module._parallel_download_disabled_for_url(
        "https://files.blocked.example/provider.json"
    )
    assert not utils_module._parallel_download_disabled_for_url(
        "https://www.example.com/provider.json"
    )


def test_extract_plan_years_from_years_array():
    payload = {"years": [2024, "2025", "2025.0", 2026.0, "bad", None]}
    assert process_initial._extract_plan_years(payload) == [2024, 2025, 2026]


def test_extract_plan_years_from_year_scalar():
    payload = {"year": "2026"}
    assert process_initial._extract_plan_years(payload) == [2026]


def test_extract_plan_years_invalid_or_missing():
    assert process_initial._extract_plan_years({"years": "bad"}) == []
    assert process_initial._extract_plan_years({}) == []


def test_normalize_marketplace_benefits_scalar_bool():
    rows = process_initial._normalize_marketplace_benefits(
        "12345XX9876543",
        2026,
        12345,
        [{"telemedicine": False}],
        datetime.datetime(2026, 1, 1),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["plan_id"] == "12345XX9876543"
    assert row["year"] == 2026
    assert row["issuer_id"] == 12345
    assert row["benefit_name"] == "telemedicine"
    assert row["benefit_value_bool"] is False
    assert row["benefit_value_text"] == "false"
    assert row["benefit_item_json"] == {"telemedicine": False}


def test_normalize_marketplace_benefits_named_value_shape():
    rows = process_initial._normalize_marketplace_benefits(
        "12345XX9876543",
        2026,
        12345,
        [{"name": "virtual_primary_care", "value": True, "label": "Virtual Primary Care"}],
        datetime.datetime(2026, 1, 1),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["benefit_name"] == "virtual_primary_care"
    assert row["benefit_label"] == "Virtual Primary Care"
    assert row["benefit_value_bool"] is True


def test_normalize_marketplace_address_entry_accepts_address2():
    row = process_initial._normalize_marketplace_address_entry(
        {
            "address": "123 Main St",
            "address2": "Suite 5",
            "city": "Austin",
            "state": "tx",
            "zip": "78701",
            "phone": "5125550000",
        }
    )

    assert row is not None
    assert row["first_line"] == "123 Main St"
    assert row["second_line"] == "Suite 5"
    assert row["city_name"] == "AUSTIN"
    assert row["state_name"] == "TX"
    assert row["postal_code"] == "78701"
    assert row["telephone_number"] == "5125550000"


def test_build_mrf_address_rows_creates_address_and_evidence():
    address_rows, evidence_rows = process_initial._build_mrf_address_rows(
        {
            "npi": "1234567890",
            "addresses": [
                {
                    "address": "123 Main St",
                    "address2": "Suite 5",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "5125550000",
                }
            ],
        },
        {
            1: {
                "issuer_id": 12345,
                "year": 2026,
                "checksum_network": 111,
                "network_tier": "PREFERRED",
            },
            2: {
                "issuer_id": 54321,
                "year": 2026,
                "checksum_network": 222,
                "network_tier": "NON-PREFERRED",
            },
        },
        "20260402",
        "https://issuer.example/providers.json",
        datetime.datetime(2026, 1, 1),
        issuer_lookup={12345: "Alpha Health Plan", 54321: "Beta Health Plan"},
    )

    assert len(address_rows) == 1
    assert len(evidence_rows) == 2
    address_row = address_rows[0]
    assert address_row["npi"] == 1234567890
    assert address_row["type"] == "practice"
    assert "source_count" not in address_row
    assert "address_sources" not in address_row
    assert "source_import_dates" not in address_row
    assert "source_issuer_ids" not in address_row
    assert "source_issuer_names" not in address_row
    assert "source_urls" not in address_row
    expected_address_key = process_initial.address_key_v1(
        "123 Main St",
        "Suite 5",
        "AUSTIN",
        "TX",
        "78701",
        "US",
    )
    assert address_row["address_key"] == expected_address_key
    evidence_row = sorted(evidence_rows, key=lambda item: item["issuer_id"])[0]
    assert evidence_row["issuer_name"] == "Alpha Health Plan"
    assert evidence_row["import_date"] == datetime.date(2026, 4, 2)
    assert evidence_row["address_key"] == expected_address_key


def test_build_mrf_address_rows_batches_contact_normalization(monkeypatch):
    seen_batches = []

    def fake_canonicalize_contact_batch(rows):
        rows = list(rows)
        seen_batches.append(rows)
        return [
            {
                "phone_number": "5125550000",
                "phone_extension": None,
                "fax_number_digits": "5125550199",
                "fax_extension": None,
            },
            {
                "phone_number": "5125550001",
                "phone_extension": "12",
                "fax_number_digits": None,
                "fax_extension": None,
            },
        ]

    monkeypatch.setattr(process_initial, "canonicalize_contact_batch", fake_canonicalize_contact_batch)

    address_rows, _evidence_rows = process_initial._build_mrf_address_rows(
        {
            "npi": "1234567890",
            "addresses": [
                {
                    "address": "123 Main St",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "512-555-0000",
                    "fax": "512-555-0199",
                },
                {
                    "address": "124 Main St",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "512-555-0001 x12",
                },
            ],
        },
        {
            1: {
                "issuer_id": 12345,
                "year": 2026,
                "checksum_network": 111,
                "network_tier": "PREFERRED",
            },
        },
        "20260601000000",
        "https://example.test/provider.json",
        datetime.datetime(2026, 6, 1, 12, 0, 0),
    )

    assert seen_batches == [
        [
            ("512-555-0000", "512-555-0199", "US"),
            ("512-555-0001 x12", None, "US"),
        ]
    ]
    assert [row["phone_number"] for row in address_rows] == ["5125550000", "5125550001"]
    assert address_rows[1]["phone_extension"] == "12"


@pytest.mark.asyncio
async def test_push_mrf_address_rows_skips_aggregate_ingest_by_default(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    await process_initial._push_mrf_address_rows(
        [{"npi": 1234567890, "type": "practice", "checksum": 1}],
        SimpleNamespace(__tablename__="mrf_address_20260612"),
    )

    assert calls == []


@pytest.mark.asyncio
async def test_push_mrf_address_rows_uses_insert_do_nothing_when_enabled(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", "1")
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="mrf_address_20260612")
    rows = [
        {
            "npi": 1234567890,
            "type": "practice",
            "checksum": 1,
            "first_line": "123 Main St",
            "source_count": 2,
            "source_issuer_ids": [12345, 54321],
            "source_urls": ["https://issuer.example/providers.json"],
        }
    ]
    await process_initial._push_mrf_address_rows(rows, cls)

    assert calls == [
        (
            [{"npi": 1234567890, "type": "practice", "checksum": 1, "first_line": "123 Main St"}],
            cls,
            {"rewrite": False, "use_copy": False},
        )
    ]


@pytest.mark.asyncio
async def test_push_mrf_duplicate_tolerant_rows_skips_copy_first(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.delenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", raising=False)
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="plan_npi_raw_20260612")
    rows = [{"npi": 1234567890, "checksum_network": 42}]
    await process_initial._push_mrf_duplicate_tolerant_rows(rows, cls)

    assert calls == [(rows, cls, {"rewrite": False, "use_copy": False})]


@pytest.mark.asyncio
async def test_push_mrf_duplicate_tolerant_rows_can_restore_copy_first(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.setenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", "1")
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="plan_npi_raw_20260612")
    rows = [{"npi": 1234567890, "checksum_network": 42}]
    await process_initial._push_mrf_duplicate_tolerant_rows(rows, cls)

    assert calls == [(rows, cls, {})]


@pytest.mark.asyncio
async def test_save_mrf_data_skips_mrf_address_aggregate_ingest(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((cls.__tablename__, rows, kwargs))

    async def fake_ensure_database(_test_mode):
        return None

    def fake_make_class(cls, suffix, schema_override=None):
        return SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}")

    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_initial, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)

    rows = [{"npi": 1234567890, "type": "practice", "checksum": 1}]
    await process_initial.save_mrf_data(
        {"context": {"import_date": "20260612", "test_mode": False}},
        {"mrf_address": rows, "mrf_address_evidence": [{"evidence_checksum": 2}]},
    )

    assert calls == [("mrf_address_evidence_20260612", [{"evidence_checksum": 2}], {})]
