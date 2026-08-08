# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redis startup contracts for the released exact-wave worker."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from process import ptg_wave_worker


def _identity() -> SimpleNamespace:
    return SimpleNamespace(
        queue="arq:PTGSmall:wave:" + "a" * 64,
        slot_index=4,
    )


class _ClosingRedisPool:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self, *, close_connection_pool: bool) -> None:
        assert close_connection_pool is True
        self.closed = True


class _SyncClosingRedisPool:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_released_worker_pool_preserves_queue_and_serializers() -> None:
    identity = _identity()
    redis_pool = _ClosingRedisPool()
    worker_calls: list[tuple[type, dict]] = []

    class BaseSettings:
        redis_settings = object()
        job_serializer = object()
        job_deserializer = object()

    class AsyncWorker:
        async def async_run(self):
            return "ran"

    def create_worker(worker_settings, **worker_options):
        worker_calls.append((worker_settings, worker_options))
        return AsyncWorker()

    pool_creator = AsyncMock(return_value=redis_pool)
    with (
        patch.dict("os.environ", {"HLTHPRT_PTG_WAVE_WORKER_SETTINGS": "process.PTGSmall"}),
        patch.object(ptg_wave_worker, "import_string", return_value=BaseSettings),
        patch.object(ptg_wave_worker, "create_ptg_wave_redis_pool", new=pool_creator),
        patch.object(ptg_wave_worker, "create_worker", side_effect=create_worker),
    ):
        assert asyncio.run(ptg_wave_worker._drain_wave_queue(identity)) == "ran"

    pool_options_by_name = pool_creator.await_args.kwargs["pool_options"]
    assert pool_creator.await_args.args == (identity,)
    assert pool_creator.await_args.kwargs["settings_factory"]() is BaseSettings.redis_settings
    assert pool_options_by_name == {
        "job_serializer": BaseSettings.job_serializer,
        "job_deserializer": BaseSettings.job_deserializer,
        "default_queue_name": identity.queue,
    }
    assert worker_calls[0][1]["redis_pool"] is redis_pool
    assert redis_pool.closed


def test_released_worker_does_not_construct_arq_after_redis_exhaustion() -> None:
    identity = _identity()

    class BaseSettings:
        redis_settings = object()

    pool_creator = AsyncMock(
        side_effect=ConnectionRefusedError("synthetic startup exhausted")
    )
    worker_creator = Mock()
    with (
        patch.dict("os.environ", {"HLTHPRT_PTG_WAVE_WORKER_SETTINGS": "process.PTGSmall"}),
        patch.object(ptg_wave_worker, "import_string", return_value=BaseSettings),
        patch.object(ptg_wave_worker, "create_ptg_wave_redis_pool", new=pool_creator),
        patch.object(ptg_wave_worker, "create_worker", new=worker_creator),
        pytest.raises(ConnectionRefusedError, match="startup exhausted"),
    ):
        asyncio.run(ptg_wave_worker._drain_wave_queue(identity))

    pool_creator.assert_awaited_once()
    worker_creator.assert_not_called()


def test_released_worker_closes_pool_when_arq_startup_fails() -> None:
    identity = _identity()
    redis_pool = _ClosingRedisPool()

    class BaseSettings:
        redis_settings = object()

    class FailingWorker:
        async def async_run(self):
            raise RuntimeError("synthetic ARQ startup failure")

    with (
        patch.dict("os.environ", {"HLTHPRT_PTG_WAVE_WORKER_SETTINGS": "process.PTGSmall"}),
        patch.object(ptg_wave_worker, "import_string", return_value=BaseSettings),
        patch.object(
            ptg_wave_worker,
            "create_ptg_wave_redis_pool",
            new=AsyncMock(return_value=redis_pool),
        ),
        patch.object(ptg_wave_worker, "create_worker", return_value=FailingWorker()),
        pytest.raises(RuntimeError, match="ARQ startup failure"),
    ):
        asyncio.run(ptg_wave_worker._drain_wave_queue(identity))

    assert redis_pool.closed


def test_released_worker_accepts_zero_argument_sync_pool_close() -> None:
    identity = _identity()
    redis_pool = _SyncClosingRedisPool()

    class BaseSettings:
        redis_settings = object()

    class AsyncWorker:
        async def async_run(self):
            return "ran"

    with (
        patch.dict("os.environ", {"HLTHPRT_PTG_WAVE_WORKER_SETTINGS": "process.PTGSmall"}),
        patch.object(ptg_wave_worker, "import_string", return_value=BaseSettings),
        patch.object(
            ptg_wave_worker,
            "create_ptg_wave_redis_pool",
            new=AsyncMock(return_value=redis_pool),
        ),
        patch.object(ptg_wave_worker, "create_worker", return_value=AsyncWorker()),
    ):
        assert asyncio.run(ptg_wave_worker._drain_wave_queue(identity)) == "ran"

    assert redis_pool.closed
