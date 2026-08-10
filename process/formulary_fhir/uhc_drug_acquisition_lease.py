# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable, token-fenced ownership for one UHC drug acquisition pass."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable
from typing import Any

from db.models import db
from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.uhc_drug_acquisition_lease_contract import (
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
    DEFAULT_LEASE_SECONDS,
    FAILURE_DRAIN_WINDOW_SECONDS,
    LeaseOperation,
    ResultT,
    UHCDrugSourceAcquisitionClaim,
    UHCDrugSourceAcquisitionLeaseError,
    _lease_seconds,
    _positive_seconds,
    _set_action,
    _validate_supervision_window,
)
from process.formulary_fhir.uhc_drug_acquisition_lease_store import (
    claim_uhc_drug_source_acquisition,
    heartbeat_uhc_drug_source_acquisition,
    release_uhc_drug_source_acquisition,
    require_active_uhc_drug_source_acquisition,
)


async def _heartbeat_loop(
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
    lease_seconds: int,
    heartbeat_seconds: float,
    heartbeat_timeout_seconds: float,
) -> None:
    while True:
        await asyncio.sleep(heartbeat_seconds)
        try:
            async with asyncio.timeout(heartbeat_timeout_seconds):
                await heartbeat_uhc_drug_source_acquisition(
                    claim,
                    lease_seconds=lease_seconds,
                    database=database,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise UHCDrugSourceAcquisitionLeaseError("lease_lost") from None


async def _join_tasks(*tasks: asyncio.Task[Any]) -> None:
    await asyncio.gather(*tasks, return_exceptions=True)


async def _stop_heartbeat(heartbeat_task: asyncio.Task[None]) -> None:
    heartbeat_task.cancel()
    await drain_operation(
        _join_tasks(heartbeat_task),
        preserve_cancellation=False,
    )


async def _best_effort_release(
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
) -> None:
    with contextlib.suppress(BaseException):
        await drain_operation(
            release_uhc_drug_source_acquisition(claim, database=database),
            preserve_cancellation=False,
        )


async def _stop_heartbeat_and_release(
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
) -> None:
    """Finish successful ownership cleanup as one cancellation-safe unit."""

    await _stop_heartbeat(heartbeat_task)
    await release_uhc_drug_source_acquisition(claim, database=database)


async def _stop_heartbeat_and_best_effort_release(
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
) -> None:
    """Stop supervision before attempting failure-path fenced release."""

    await _stop_heartbeat(heartbeat_task)
    await _best_effort_release(claim, database=database)


_DETACHED_DRAIN_TASKS: set[asyncio.Task[None]] = set()


def _consume_background_task(background_task: asyncio.Task[Any]) -> None:
    _DETACHED_DRAIN_TASKS.discard(background_task)
    with contextlib.suppress(BaseException):
        background_task.exception()


def _retain_background_drain(operation: Awaitable[None]) -> None:
    background_task = asyncio.create_task(operation)
    _DETACHED_DRAIN_TASKS.add(background_task)
    background_task.add_done_callback(_consume_background_task)


def _is_heartbeat_lost(heartbeat_task: asyncio.Task[None]) -> bool:
    if not heartbeat_task.done():
        return False
    with contextlib.suppress(BaseException):
        heartbeat_task.exception()
    return True


async def _finish_owned_detached_drain(
    operation_task: asyncio.Task[Any],
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
) -> None:
    completed, _pending = await asyncio.wait(
        (operation_task, heartbeat_task),
        return_when=asyncio.FIRST_COMPLETED,
    )
    if heartbeat_task in completed:
        await _join_tasks(operation_task, heartbeat_task)
        return
    await _join_tasks(operation_task)
    await _stop_heartbeat(heartbeat_task)
    await _best_effort_release(claim, database=database)


async def _finish_unowned_detached_drain(
    operation_task: asyncio.Task[Any],
    heartbeat_task: asyncio.Task[None],
) -> None:
    await _join_tasks(operation_task, heartbeat_task)


async def _cancel_operation_under_lease(
    operation_task: asyncio.Task[Any],
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
    failure_drain_seconds: float,
) -> None:
    """Cancel once, bound caller wait, and retain ownership until drained."""

    operation_task.cancel()
    deadline = asyncio.get_running_loop().time() + failure_drain_seconds
    is_heartbeat_lost = _is_heartbeat_lost(heartbeat_task)
    while not operation_task.done():
        remaining_seconds = deadline - asyncio.get_running_loop().time()
        if remaining_seconds <= 0:
            break
        watched_tasks: set[asyncio.Task[Any]] = {operation_task}
        if not heartbeat_task.done():
            watched_tasks.add(heartbeat_task)
        completed, _pending = await asyncio.wait(
            watched_tasks,
            timeout=remaining_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not completed:
            break
        is_heartbeat_lost = is_heartbeat_lost or _is_heartbeat_lost(heartbeat_task)

    if not operation_task.done():
        if is_heartbeat_lost:
            _retain_background_drain(
                _finish_unowned_detached_drain(
                    operation_task,
                    heartbeat_task,
                )
            )
        else:
            _retain_background_drain(
                _finish_owned_detached_drain(
                    operation_task,
                    heartbeat_task,
                    claim,
                    database=database,
                )
            )
        return

    await _join_tasks(operation_task)
    is_heartbeat_lost = is_heartbeat_lost or _is_heartbeat_lost(heartbeat_task)
    if is_heartbeat_lost:
        await _best_effort_release(claim, database=database)
    else:
        await _stop_heartbeat(heartbeat_task)
        await _best_effort_release(claim, database=database)


def _supervision_settings(
    lease_seconds: int,
    heartbeat_seconds: float,
    heartbeat_timeout_seconds: float,
    failure_drain_seconds: float,
) -> tuple[int, float, float, float]:
    normalized_settings = (
        _lease_seconds(lease_seconds),
        _positive_seconds(heartbeat_seconds, "heartbeat interval"),
        _positive_seconds(heartbeat_timeout_seconds, "heartbeat timeout"),
        _positive_seconds(failure_drain_seconds, "failure drain window"),
    )
    _validate_supervision_window(*normalized_settings)
    return normalized_settings


async def _claimed_tasks(
    source_id: str,
    operation: LeaseOperation[ResultT],
    *,
    database: Any,
    lease_seconds: int,
    heartbeat_seconds: float,
    heartbeat_timeout_seconds: float,
) -> tuple[
    UHCDrugSourceAcquisitionClaim,
    asyncio.Task[ResultT],
    asyncio.Task[None],
]:
    claim = await claim_uhc_drug_source_acquisition(
        source_id,
        lease_seconds=lease_seconds,
        database=database,
    )
    operation_task = asyncio.create_task(operation(claim))
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            claim,
            database=database,
            lease_seconds=lease_seconds,
            heartbeat_seconds=heartbeat_seconds,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )
    )
    return claim, operation_task, heartbeat_task


async def _drain_failed_operation(
    operation_task: asyncio.Task[Any],
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
    failure_drain_seconds: float,
) -> None:
    await drain_operation(
        _cancel_operation_under_lease(
            operation_task,
            heartbeat_task,
            claim,
            database=database,
            failure_drain_seconds=failure_drain_seconds,
        ),
        preserve_cancellation=False,
    )


async def _wait_for_operation(
    operation_task: asyncio.Task[Any],
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
    failure_drain_seconds: float,
) -> None:
    try:
        completed, _pending = await asyncio.wait(
            (operation_task, heartbeat_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        await _drain_failed_operation(
            operation_task,
            heartbeat_task,
            claim,
            database=database,
            failure_drain_seconds=failure_drain_seconds,
        )
        raise
    if heartbeat_task in completed:
        await _drain_failed_operation(
            operation_task,
            heartbeat_task,
            claim,
            database=database,
            failure_drain_seconds=failure_drain_seconds,
        )
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost") from None


async def _finish_operation(
    operation_task: asyncio.Task[ResultT],
    heartbeat_task: asyncio.Task[None],
    claim: UHCDrugSourceAcquisitionClaim,
    *,
    database: Any,
) -> ResultT:
    try:
        operation_result = operation_task.result()
    except BaseException:
        await drain_operation(
            _stop_heartbeat_and_best_effort_release(
                heartbeat_task,
                claim,
                database=database,
            ),
            preserve_cancellation=False,
        )
        raise
    await drain_operation(
        _stop_heartbeat_and_release(
            heartbeat_task,
            claim,
            database=database,
        ),
        preserve_cancellation=True,
    )
    return operation_result


async def run_with_source_lease(
    source_id: str,
    operation: LeaseOperation[ResultT],
    *,
    database: Any = db,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    heartbeat_timeout_seconds: float = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
    failure_drain_seconds: float = FAILURE_DRAIN_WINDOW_SECONDS,
) -> ResultT:
    """Supervise one operation until its fenced claim is cleanly released."""

    normalized_settings = _supervision_settings(
        lease_seconds,
        heartbeat_seconds,
        heartbeat_timeout_seconds,
        failure_drain_seconds,
    )
    if not callable(operation):
        raise ValueError("FHIR formulary source acquisition operation is invalid")
    claim, operation_task, heartbeat_task = await _claimed_tasks(
        source_id,
        operation,
        database=database,
        lease_seconds=normalized_settings[0],
        heartbeat_seconds=normalized_settings[1],
        heartbeat_timeout_seconds=normalized_settings[2],
    )
    await _wait_for_operation(
        operation_task,
        heartbeat_task,
        claim,
        database=database,
        failure_drain_seconds=normalized_settings[3],
    )
    return await _finish_operation(
        operation_task,
        heartbeat_task,
        claim,
        database=database,
    )


run_with_uhc_drug_source_acquisition_lease = run_with_source_lease


__all__ = (
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_HEARTBEAT_TIMEOUT_SECONDS",
    "DEFAULT_LEASE_SECONDS",
    "FAILURE_DRAIN_WINDOW_SECONDS",
    "UHCDrugSourceAcquisitionClaim",
    "UHCDrugSourceAcquisitionLeaseError",
    "claim_uhc_drug_source_acquisition",
    "heartbeat_uhc_drug_source_acquisition",
    "release_uhc_drug_source_acquisition",
    "require_active_uhc_drug_source_acquisition",
    "run_with_uhc_drug_source_acquisition_lease",
)
