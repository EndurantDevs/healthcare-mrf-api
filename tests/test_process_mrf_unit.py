# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")


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


def test_mrf_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.MRF_finish.functions]
    assert names == ["shutdown"]
    assert process_pkg.MRF_finish.queue_name == "arq:MRF_finish"


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

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.plan_attributes.callback(test=True)

    fake_initiate.assert_called_once_with(test_mode=True)

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

    await process_initial.finish_main()

    fake_pool.enqueue_job.assert_awaited_once_with("shutdown", _queue_name="arq:MRF_finish")


@pytest.mark.asyncio
async def test_refresh_do_business_as(monkeypatch):
    calls = []

    async def fake_status(sql):
        calls.append(sql)

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", fake_status)
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    await process_npi.refresh_do_business_as()

    sql_strings = [str(sql) for sql in calls]
    assert any("SET do_business_as = ARRAY[]::varchar[], do_business_as_text = ''" in s for s in sql_strings)
    assert any("do_business_as_text = COALESCE" in str(sql) for sql in calls)
