# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Entrypoint for one released PTG wave slot.

The configured barrier factory is intentionally required.  Until the durable
Redis release primitive is wired by the controller work, this entrypoint fails
closed rather than constructing an ARQ worker against any queue.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
from typing import Any, Callable

from arq.utils import import_string
from arq.worker import create_worker

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    PTG_WAVE_WORKER_CLASS,
)
from process.ptg_wave_barrier import PTGWaveBarrier, PTGWaveWorkerIdentity, run_after_wave_release


async def run_wave_worker(
    *,
    barrier_factory: Callable[[PTGWaveWorkerIdentity], PTGWaveBarrier | Any] | None = None,
    worker_runner: Callable[[PTGWaveWorkerIdentity], Any] | None = None,
) -> Any:
    """Drain the dedicated queue after a verified release, one job per slot."""

    identity = PTGWaveWorkerIdentity.from_environment()
    barrier = await _resolve_barrier(identity, barrier_factory)
    runner = worker_runner or _drain_wave_queue
    return await run_after_wave_release(identity, barrier, lambda: runner(identity))


async def _resolve_barrier(
    identity: PTGWaveWorkerIdentity,
    factory_override: Callable[[PTGWaveWorkerIdentity], PTGWaveBarrier | Any] | None,
) -> PTGWaveBarrier:
    factory = factory_override or _factory_from_environment()
    barrier = factory(identity)
    if inspect.isawaitable(barrier):
        barrier = await barrier
    if not callable(getattr(barrier, "register_ready", None)) or not callable(getattr(barrier, "wait_for_release", None)):
        raise PTGWaveContractError("wave barrier factory did not return a ready/release barrier")
    return barrier


def _factory_from_environment() -> Callable[[PTGWaveWorkerIdentity], PTGWaveBarrier]:
    dotted = os.getenv("HLTHPRT_PTG_WAVE_BARRIER_FACTORY", "").strip()
    if not dotted or "." not in dotted:
        raise PTGWaveContractError("HLTHPRT_PTG_WAVE_BARRIER_FACTORY is required")
    module_name, _, attribute = dotted.rpartition(".")
    try:
        factory = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError) as exc:
        raise PTGWaveContractError("configured wave barrier factory is unavailable") from exc
    if not callable(factory):
        raise PTGWaveContractError("configured wave barrier factory is not callable")
    return factory


async def _drain_wave_queue(identity: PTGWaveWorkerIdentity) -> Any:
    """Construct one-concurrent-job ARQ worker only after release validation."""

    settings_path = os.getenv("HLTHPRT_PTG_WAVE_WORKER_SETTINGS", "").strip()
    if settings_path != PTG_WAVE_WORKER_CLASS:
        raise PTGWaveContractError("wave worker settings must be process.PTGSmall")
    base_settings = import_string(settings_path)
    wave_settings = type(
        "ReleasedPTGWaveSmall",
        (base_settings,),
        {
            "queue_name": identity.queue,
            "max_jobs": 1,
            # ARQ workers all scan from sorted-set offset zero.  A one-item
            # window makes twelve Pods contend for the same head job and
            # serializes the wave.  The twelve-item window lets each Pod claim
            # one distinct in-progress key while max_jobs retains one-at-a-time
            # execution inside every Pod.
            "queue_read_limit": PTG_WAVE_SLOT_COUNT,
        },
    )
    worker = create_worker(
        wave_settings,
        burst=True,
        max_jobs=1,
        queue_read_limit=PTG_WAVE_SLOT_COUNT,
    )
    return await worker.async_run()


if __name__ == "__main__":
    asyncio.run(run_wave_worker())
