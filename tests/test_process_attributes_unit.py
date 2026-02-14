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
        payload = call.args[1]
        assert payload["context"]["test_mode"] is True


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
