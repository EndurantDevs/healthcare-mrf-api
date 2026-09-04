from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports


@pytest.mark.asyncio
async def test_pipeline_teardown_cannot_hold_the_database_lock(monkeypatch):
    release_teardown = asyncio.Event()
    pool_closed = asyncio.Event()
    pipeline = Mock()
    pipeline.__aenter__ = AsyncMock(return_value=pipeline)
    async def hanging_exit(*_args):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release_teardown.wait()

    async def close_pool(**_kwargs):
        pool_closed.set()
        release_teardown.set()

    pipeline.__aexit__ = AsyncMock(side_effect=hanging_exit)
    redis_pool = Mock()
    redis_pool.pipeline.return_value = pipeline
    redis_pool.aclose = AsyncMock(side_effect=close_pool)
    monkeypatch.setattr(
        control_imports,
        "build_redis_settings",
        lambda: SimpleNamespace(conn_timeout=0.01),
    )
    monkeypatch.setattr(
        control_imports,
        "create_pool",
        AsyncMock(return_value=redis_pool),
    )
    monkeypatch.setattr(
        control_imports,
        "_reconcile_terminal_queue_member",
        AsyncMock(return_value={}),
    )
    request_task = asyncio.create_task(
        control_imports._remove_terminal_queue_residue(
            {
                "run_id": "run-terminal",
                "importer": "plan-pricing-projection",
                "status": "failed",
                "metrics": {
                    "queue": "arq:PTGCandidateAudit",
                    "job_id": "plan_pricing_projection_run-terminal",
                },
            },
            {
                "expected_importer": "plan-pricing-projection",
                "expected_status": "failed",
            },
        )
    )
    try:
        await asyncio.wait_for(pool_closed.wait(), timeout=0.5)
    finally:
        release_teardown.set()
    failure = (await asyncio.gather(request_task, return_exceptions=True))[0]

    assert isinstance(failure, control_imports.StaleWorkerReconciliationUnavailable)
    redis_pool.aclose.assert_awaited_once_with(close_connection_pool=True)
