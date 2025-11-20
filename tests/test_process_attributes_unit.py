# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

process_attributes = importlib.import_module("process.attributes")


@pytest.mark.asyncio
async def test_plan_attributes_main_enqueues_test_context(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(
        process_attributes,
        "create_pool",
        AsyncMock(return_value=fake_pool),
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

    assert fake_pool.enqueue_job.await_count == 4
    for call in fake_pool.enqueue_job.await_args_list:
        payload = call.args[1]
        assert payload["context"]["test_mode"] is True
