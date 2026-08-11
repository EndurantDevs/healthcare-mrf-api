# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from unittest.mock import ANY, AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_acquisition_lease as lease


SOURCE_ID = "uhc-official-formulary-mrf"
TOKEN = "a" * 64


def _claim(generation: int = 1) -> lease.UHCDrugSourceAcquisitionClaim:
    return lease.UHCDrugSourceAcquisitionClaim(
        source_id=SOURCE_ID,
        lease_generation=generation,
        lease_token=TOKEN,
    )


def _supervision(operation, **overrides):
    supervision_by_option = {
        "database": object(),
        "lease_seconds": 3,
        "heartbeat_seconds": 0.1,
        "heartbeat_timeout_seconds": 0.1,
        "failure_drain_seconds": 0.1,
    }
    supervision_by_option.update(overrides)
    return lease.run_with_uhc_drug_source_acquisition_lease(
        SOURCE_ID,
        operation,
        **supervision_by_option,
    )


async def _wait_for_release(release: AsyncMock) -> None:
    for _attempt in range(100):
        if release.await_count == 1:
            return
        await asyncio.sleep(0.01)


async def _wait_for_detached_drains() -> None:
    for _attempt in range(100):
        if not lease._DETACHED_DRAIN_TASKS:
            return
        await asyncio.sleep(0.01)


def test_claim_repr_and_error_never_expose_the_fence_token() -> None:
    claim = _claim()
    error = lease.UHCDrugSourceAcquisitionLeaseError("busy")

    assert TOKEN not in repr(claim)
    assert claim.source_id in repr(claim)
    assert str(error) == "UHC drug source acquisition lease failed"
    assert error.code == "busy"


@pytest.mark.asyncio
async def test_supervisor_releases_the_exact_successful_generation(
    monkeypatch,
) -> None:
    claim = _claim()
    release = AsyncMock()
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def operation(observed_claim):
        assert observed_claim is claim
        return "complete"

    acquisition_result = await _supervision(operation)

    assert acquisition_result == "complete"
    release.assert_awaited_once_with(claim, database=ANY)


@pytest.mark.asyncio
async def test_cancellation_during_successful_heartbeat_cleanup_is_preserved(
    monkeypatch,
) -> None:
    claim = _claim()
    heartbeat_cleanup_started = asyncio.Event()
    allow_heartbeat_cleanup = asyncio.Event()
    release = AsyncMock()
    original_join_tasks = lease._join_tasks
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def block_heartbeat_join(*tasks):
        """Expose the exact post-success heartbeat-drain race."""

        heartbeat_cleanup_started.set()
        await allow_heartbeat_cleanup.wait()
        await original_join_tasks(*tasks)

    monkeypatch.setattr(lease, "_join_tasks", block_heartbeat_join)

    async def operation(_claim):
        return "complete"

    acquisition = asyncio.create_task(_supervision(operation))
    await heartbeat_cleanup_started.wait()
    acquisition.cancel()
    allow_heartbeat_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await acquisition
    release.assert_awaited_once_with(claim, database=ANY)


@pytest.mark.asyncio
async def test_heartbeat_loss_cancels_and_drains_the_claimed_operation(
    monkeypatch,
) -> None:
    claim = _claim()
    operation_started = asyncio.Event()
    operation_cleaned = asyncio.Event()
    release = AsyncMock()
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        AsyncMock(side_effect=lease.UHCDrugSourceAcquisitionLeaseError("lease_lost")),
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def operation(_claim):
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            operation_cleaned.set()

    acquisition = asyncio.create_task(_supervision(operation, heartbeat_seconds=0.01))
    await operation_started.wait()

    with pytest.raises(
        lease.UHCDrugSourceAcquisitionLeaseError,
        match="source acquisition lease failed",
    ) as caught:
        await acquisition

    assert caught.value.code == "lease_lost"
    assert operation_cleaned.is_set()
    release.assert_awaited_once_with(claim, database=ANY)


@pytest.mark.asyncio
async def test_outer_cancellation_drains_cleanup_before_fenced_release(
    monkeypatch,
) -> None:
    claim = _claim()
    operation_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    release = AsyncMock()
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def operation(_claim):
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            cleanup_finished.set()

    acquisition = asyncio.create_task(_supervision(operation))
    await operation_started.wait()
    acquisition.cancel()
    await cleanup_started.wait()
    acquisition.cancel()
    allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await acquisition
    assert cleanup_finished.is_set()
    release.assert_awaited_once_with(claim, database=ANY)


@pytest.mark.asyncio
async def test_slow_cancel_keeps_heartbeating_until_detached_drain_finishes(
    monkeypatch,
) -> None:
    """A slow cancellation retains heartbeats and releases after final drain."""

    claim = _claim()
    operation_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    heartbeat = AsyncMock()
    release = AsyncMock()
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        heartbeat,
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def operation(_claim):
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            cleanup_finished.set()

    acquisition = asyncio.create_task(
        _supervision(
            operation,
            heartbeat_seconds=0.005,
            failure_drain_seconds=0.02,
        )
    )
    await operation_started.wait()
    acquisition.cancel()
    await cleanup_started.wait()

    with pytest.raises(asyncio.CancelledError):
        await acquisition
    assert release.await_count == 0
    assert heartbeat.await_count >= 1

    allow_cleanup.set()
    await _wait_for_release(release)
    assert cleanup_finished.is_set()
    release.assert_awaited_once_with(claim, database=ANY)
    assert not lease._DETACHED_DRAIN_TASKS


@pytest.mark.asyncio
async def test_heartbeat_loss_during_cancel_drain_is_observed_and_unowned(
    monkeypatch,
) -> None:
    """Heartbeat loss prevents release by the detached stale generation."""

    claim = _claim()
    operation_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    heartbeat = AsyncMock(
        side_effect=lease.UHCDrugSourceAcquisitionLeaseError("lease_lost")
    )
    release = AsyncMock()
    monkeypatch.setattr(
        lease,
        "claim_uhc_drug_source_acquisition",
        AsyncMock(return_value=claim),
    )
    monkeypatch.setattr(
        lease,
        "heartbeat_uhc_drug_source_acquisition",
        heartbeat,
    )
    monkeypatch.setattr(
        lease,
        "release_uhc_drug_source_acquisition",
        release,
    )

    async def operation(_claim):
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            cleanup_finished.set()

    acquisition = asyncio.create_task(
        _supervision(
            operation,
            heartbeat_seconds=0.005,
            failure_drain_seconds=0.02,
        )
    )
    await operation_started.wait()
    acquisition.cancel()
    await cleanup_started.wait()

    with pytest.raises(asyncio.CancelledError):
        await acquisition
    assert heartbeat.await_count == 1
    assert release.await_count == 0

    allow_cleanup.set()
    await _wait_for_detached_drains()
    assert cleanup_finished.is_set()
    assert release.await_count == 0
    assert not lease._DETACHED_DRAIN_TASKS


def test_supervision_rejects_a_ttl_inside_the_failure_drain_window() -> None:
    with pytest.raises(ValueError, match="supervision window"):
        lease._validate_supervision_window(100, 30.0, 15.0, 60.0)
