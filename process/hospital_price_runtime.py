# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared resource and cancellation guards for hospital-price imports."""

from __future__ import annotations

import asyncio
import os
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Sequence

from process.control_cancel import raise_if_cancelled
from process.formulary_fhir.async_safety import drain_operation
from process.hospital_price_acquisition import (
    MAX_HOSPITAL_HPT_LOCATOR_BYTES,
    PTG2_DEFAULT_MAX_BYTES,
    positive_env,
)
from process.hospital_price_store import renew_attempt_leases
from process.hospital_price_native import HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV
from process.live_progress import enqueue_live_progress
from process.ptg_parts.artifacts import PTG2ArtifactStore


HOSPITAL_MRF_SELECTOR_KEY_MEMORY_BYTES = 256 * 1024**2
HOSPITAL_MRF_PARSER_BASE_MEMORY_BYTES = 256 * 1024**2
DEFAULT_FETCH_CONCURRENCY = 8
DEFAULT_LOAD_CONCURRENCY = 2
HOSPITAL_PRICE_ARTIFACT_DIR_ENV = "HLTHPRT_HOSPITAL_PRICE_ARTIFACT_DIR"


def hospital_price_artifact_store() -> PTG2ArtifactStore:
    """Open the dedicated shared hospital-price artifact volume."""

    raw_root = os.getenv(HOSPITAL_PRICE_ARTIFACT_DIR_ENV, "").strip()
    root = Path(raw_root)
    if not raw_root or not root.is_absolute():
        raise RuntimeError(
            f"{HOSPITAL_PRICE_ARTIFACT_DIR_ENV} must be an absolute path"
        )
    return PTG2ArtifactStore(root)


def strict_positive_env(name: str, default: int | None = None) -> int:
    """Read one required positive integer environment value."""

    raw_value = os.getenv(name)
    if raw_value is None and default is not None:
        return default
    try:
        value = int(raw_value or "")
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def resource_limits(
    store: PTG2ArtifactStore,
    requested_fetches: int,
    requested_loads: int,
    locator_count: int,
) -> tuple[int, int, int, int, int, int]:
    """Derive worker counts and byte limits from explicit capacity budgets."""

    max_raw = strict_positive_env(
        "HLTHPRT_HOSPITAL_MRF_MAX_BYTES", PTG2_DEFAULT_MAX_BYTES
    )
    max_output = strict_positive_env("HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES")
    max_decompressed = strict_positive_env(
        HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV
    )
    active_raw = strict_positive_env("HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES")
    active_scratch = strict_positive_env(
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES"
    )
    active_memory = strict_positive_env(
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_MEMORY_BYTES"
    )
    database_growth = strict_positive_env(
        "HLTHPRT_HOSPITAL_PRICE_DATABASE_GROWTH_BYTES"
    )
    minimum_free = strict_positive_env("HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES")
    fetches = min(requested_fetches, active_raw // max_raw)
    # Retained output is capped at max_output; selector sort scratch is capped at
    # another max_output. Reserve parser buffers plus its bounded selector keys.
    packed_peak = 2 * max_output
    parser_memory = HOSPITAL_MRF_PARSER_BASE_MEMORY_BYTES + min(
        max_output, HOSPITAL_MRF_SELECTOR_KEY_MEMORY_BYTES
    )
    loads = min(
        requested_loads,
        active_scratch // packed_peak,
        active_memory // parser_memory,
    )
    if fetches < 1 or loads < 1:
        raise RuntimeError(
            "hospital price byte and memory budgets cannot admit one source"
        )
    # DEV preflight verifies that the artifact and database paths share one
    # capacity domain. Database growth is a one-time admission reserve; workers
    # retain only the operating floor as committed data consumes that reserve.
    operating_free = (
        active_raw
        + active_scratch
        + packed_peak * loads
        + minimum_free
    )
    require_disk_capacity(
        store,
        operating_free
        + database_growth
        + locator_count * MAX_HOSPITAL_HPT_LOCATOR_BYTES,
    )
    return fetches, loads, max_raw, max_decompressed, max_output, operating_free


def configured_resource_limits(
    store: PTG2ArtifactStore, locator_count: int
) -> tuple[int, int, int, int, int, int]:
    """Apply the configured hospital worker counts to the shared capacity gate."""

    return resource_limits(
        store,
        positive_env(
            "HLTHPRT_HOSPITAL_PRICE_FETCH_CONCURRENCY",
            DEFAULT_FETCH_CONCURRENCY,
        ),
        positive_env(
            "HLTHPRT_HOSPITAL_PRICE_LOAD_CONCURRENCY",
            DEFAULT_LOAD_CONCURRENCY,
        ),
        locator_count,
    )


def require_disk_capacity(store: PTG2ArtifactStore, required_free: int) -> None:
    """Reject work unless the artifact volume has the required free bytes."""

    if shutil.disk_usage(store.root).free < required_free:
        raise RuntimeError("hospital price artifact storage capacity is insufficient")


@asynccontextmanager
async def hospital_resource_lock(store: PTG2ArtifactStore):
    """Serialize capacity admission for one shared artifact volume."""

    # ponytail: one run lock protects a shared artifact volume; replace it with
    # cross-process weighted reservations if concurrent runs become necessary.
    lock = store.named_lock("hospital-price", "resource-capacity")
    while True:
        acquire_task = asyncio.create_task(
            asyncio.to_thread(lock.try_acquire)
        )
        try:
            acquired = await asyncio.shield(acquire_task)
        except BaseException:
            while not acquire_task.done():
                try:
                    await asyncio.shield(acquire_task)
                except asyncio.CancelledError:
                    continue
            if acquire_task.result() is not None:
                lock.release()
            raise
        if acquired is not None:
            break
        await asyncio.sleep(0.1)
    try:
        yield
    finally:
        lock.release()


async def bounded(
    items: Sequence[Any], concurrency: int, operation: Any
) -> list[Any]:
    """Run operations concurrently and drain every task after failure."""

    semaphore = asyncio.Semaphore(concurrency)

    async def _run_one(item: Any) -> Any:
        """Run one operation while holding a bounded slot."""
        async with semaphore:
            return await operation(item)

    operation_tasks = [asyncio.create_task(_run_one(item)) for item in items]

    async def _join() -> list[Any]:
        return list(await asyncio.gather(*operation_tasks))

    join_task = asyncio.create_task(_join())
    try:
        return await asyncio.shield(join_task)
    except BaseException:
        await cancel_and_drain(
            operation_tasks,
            wait_for=(join_task,),
            preserve_cancellation=False,
        )
        raise


async def cancel_and_drain(
    tasks: Sequence[asyncio.Task[Any]],
    *,
    wait_for: Sequence[asyncio.Task[Any]] = (),
    preserve_cancellation: bool,
) -> None:
    """Cancel owned tasks and finish their cleanup through repeated cancellation."""

    for owned_task in tasks:
        if not owned_task.done() and owned_task.cancelling() == 0:
            owned_task.cancel()

    async def _join() -> None:
        await asyncio.gather(*tasks, *wait_for, return_exceptions=True)

    await drain_operation(_join(), preserve_cancellation=preserve_cancellation)


async def guard_cancellation(
    ctx: dict[str, Any],
    task: dict[str, Any],
    operation: Any,
    attempts: list[Any],
    lease_owner: str,
    lease_seconds: int,
    heartbeat_seconds: int,
) -> Any:
    """Monitor cancellation and renew durable attempt leases during work."""

    operation_task = asyncio.ensure_future(operation)

    async def _monitor() -> None:
        loop = asyncio.get_running_loop()
        next_heartbeat = loop.time() + heartbeat_seconds
        while True:
            await asyncio.sleep(
                positive_env("HLTHPRT_HOSPITAL_PRICE_CANCEL_POLL_SECONDS", 2)
            )
            await raise_if_cancelled(ctx, task)
            if loop.time() >= next_heartbeat:
                await renew_attempt_leases(
                    attempts,
                    lease_owner=lease_owner,
                    lease_seconds=lease_seconds,
                )
                next_heartbeat = loop.time() + heartbeat_seconds

    monitor_task = asyncio.create_task(_monitor())
    try:
        completed, _pending = await asyncio.wait(
            {operation_task, monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if operation_task in completed:
            return await operation_task
        return await monitor_task
    finally:
        await cancel_and_drain(
            (operation_task, monitor_task), preserve_cancellation=True
        )


def progress(
    run_id: str | None, phase: str, done: int, total: int, message: str
) -> None:
    """Publish one hospital-price progress update."""

    enqueue_live_progress(
        run_id=run_id,
        importer="hospital-prices",
        status="running",
        phase=phase,
        unit="hospital",
        done=done,
        total=total,
        pct=(100 * done / total if total else 100),
        message=message,
    )


def locator_groups(
    hospitals: Sequence[dict[str, str]],
) -> tuple[tuple[str, tuple[dict[str, str], ...]], ...]:
    """Group hospitals by locator while preserving registry order."""

    hospitals_by_locator: dict[str, list[dict[str, str]]] = {}
    for hospital in hospitals:
        hospitals_by_locator.setdefault(hospital["cms_hpt_url"], []).append(hospital)
    return tuple(
        (url, tuple(hospital_rows))
        for url, hospital_rows in hospitals_by_locator.items()
    )
