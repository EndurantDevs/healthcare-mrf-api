# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import importlib
import datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

process_attributes = importlib.import_module("process.attributes")


@pytest.mark.asyncio
async def test_plan_attributes_main_enqueues_test_context(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    create_pool_mock = AsyncMock(return_value=fake_pool)
    monkeypatch.setattr(
        process_attributes,
        "create_pool",
        create_pool_mock,
    )

    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF",
        json.dumps([{"url": "https://example.com/plan.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF",
        json.dumps([{"url": "https://example.com/state.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PRICE_PLAN_URL_PUF",
        json.dumps([{"url": "https://example.com/price.json", "year": "2026"}]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_BENEFITS_URL_PUF",
        json.dumps([{"url": "https://example.com/benefits.json", "year": "2026"}]),
    )
    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

    await process_attributes.main(test_mode=True)

    assert create_pool_mock.await_count == 1
    _, kwargs = create_pool_mock.await_args
    assert kwargs["default_queue_name"] == process_attributes.ATTRIBUTES_QUEUE_NAME
    assert kwargs["job_serializer"] is process_attributes.serialize_job
    assert kwargs["job_deserializer"] is process_attributes.deserialize_job
    assert fake_pool.enqueue_job.await_count == 4
    for call in fake_pool.enqueue_job.await_args_list:
        enqueued_job_payload = call.args[1]
        assert enqueued_job_payload["context"]["test_mode"] is True


@pytest.mark.asyncio
async def test_plan_attributes_control_start_runs_inline_fanout(monkeypatch):
    calls = []

    monkeypatch.setattr(
        process_attributes,
        "_attribute_source_groups",
        lambda: {
            "state_attributes": [{"url": "https://example.com/state.csv.zip", "year": "2026"}],
            "attributes": [{"url": "https://example.com/attr.csv.zip", "year": "2026"}],
            "prices": [{"url": "https://example.com/price.csv.zip", "year": "2026"}],
            "benefits": [{"url": "https://example.com/benefits.csv.zip", "year": "2026"}],
        },
    )

    async def fake_process_state(_ctx, task):
        calls.append(("state", task["context"]["test_mode"]))

    async def fake_process_attributes(ctx, task):
        calls.append(("attributes", task["context"]["test_mode"]))
        await ctx["redis"].enqueue_job("save_attributes", {"attr_obj_list": [], "context": task["context"]})

    async def fake_process_prices(_ctx, task):
        calls.append(("prices", task["context"]["test_mode"]))

    async def fake_process_benefits(_ctx, task):
        calls.append(("benefits", task["context"]["test_mode"]))

    async def fake_save(_ctx, _task):
        calls.append(("save", True))

    async def fake_shutdown(_ctx):
        calls.append(("shutdown", True))

    monkeypatch.setattr(process_attributes, "process_state_attributes", fake_process_state)
    monkeypatch.setattr(process_attributes, "process_attributes", fake_process_attributes)
    monkeypatch.setattr(process_attributes, "process_prices", fake_process_prices)
    monkeypatch.setattr(process_attributes, "process_benefits", fake_process_benefits)
    monkeypatch.setattr(process_attributes, "save_attributes", fake_save)
    monkeypatch.setattr(process_attributes, "shutdown", fake_shutdown)

    control_start_summary = await process_attributes.plan_attributes_control_start({}, {"test_mode": True})

    assert control_start_summary["test_mode"] is True
    assert control_start_summary["inline_save_jobs"] == 1
    assert calls == [
        ("state", True),
        ("attributes", True),
        ("save", True),
        ("prices", True),
        ("benefits", True),
        ("shutdown", True),
    ]


@pytest.mark.asyncio
async def test_shutdown_skips_missing_import_tables(monkeypatch):
    monkeypatch.setattr(process_attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_attributes, "get_import_schema", lambda *_args, **_kwargs: "mrf")
    monkeypatch.setattr(process_attributes.db, "scalar", AsyncMock(return_value=False))
    status_mock = AsyncMock()
    ddl_mock = AsyncMock()
    monkeypatch.setattr(process_attributes.db, "status", status_mock)
    monkeypatch.setattr(process_attributes.db, "execute_ddl", ddl_mock)

    @asynccontextmanager
    async def fake_transaction():
        yield None

    monkeypatch.setattr(process_attributes.db, "transaction", fake_transaction)

    ctx = {
        "import_date": "20260214",
        "context": {
            "test_mode": True,
            "start": datetime.datetime.utcnow(),
        },
    }
    await process_attributes.shutdown(ctx)

    status_sql = [call.args[0] for call in status_mock.await_args_list if call.args]
    assert status_mock.await_count == 0
    assert all("CREATE INDEX" not in sql for sql in status_sql)
    assert ddl_mock.await_count == 0
