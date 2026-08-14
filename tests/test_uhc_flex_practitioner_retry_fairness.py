# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fairness and cancellation checks for delayed Flex Practitioner retries."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from process.uhc_flex_practitioner_store import UHCFlexPractitionerStoreError
from process.uhc_flex_practitioner_transport import (
    UHCFlexPractitionerTransportError,
)
from tests.test_uhc_flex_practitioner_acquisition_runtime_boundaries import (
    _runner_fixture,
)


@pytest.mark.asyncio
async def test_released_retry_does_not_block_general_claims():
    runner, _harness, _context = await _runner_fixture(npi_count=2)
    first_claim = await runner.claim()
    assert first_claim is not None

    await runner.release_for_retry(first_claim)

    fresh_claim = await asyncio.wait_for(runner.claim(), timeout=0.1)
    assert fresh_claim is not None
    assert fresh_claim.requested_npi != first_claim.requested_npi


@pytest.mark.asyncio
async def test_released_retry_is_fenced_from_general_tail_until_exact_delay():
    runner, harness, _context = await _runner_fixture()
    first_claim = await runner.claim()
    assert first_claim is not None
    await runner.release_for_retry(first_claim)

    fresh_only_calls = []
    claim_work = runner.dependencies.claim_work

    async def record_claim_mode(*args, **kwargs):
        fresh_only_calls.append(kwargs.get("fresh_only"))
        return await claim_work(*args, **kwargs)

    runner.dependencies = replace(runner.dependencies, claim_work=record_claim_mode)
    assert await runner.claim() is None
    assert await runner.claim() is None
    assert fresh_only_calls == [True, False, False]
    attempt_key = (first_claim.acquisition_id, first_claim.requested_npi)
    assert harness.attempts[attempt_key] == 1

    delay_started = asyncio.Event()
    delay_finished = asyncio.Event()

    async def controlled_sleep(_delay_seconds):
        delay_started.set()
        await delay_finished.wait()

    runner.dependencies = replace(runner.dependencies, sleep=controlled_sleep)
    retry_task = asyncio.create_task(
        runner.claim_retry(first_claim.requested_npi, 1.0)
    )
    await delay_started.wait()
    assert await runner.claim() is None
    assert harness.attempts[attempt_key] == 1

    delay_finished.set()
    retry_claim = await retry_task
    assert retry_claim is not None
    assert retry_claim.requested_npi == first_claim.requested_npi
    assert retry_claim.attempt == 2


@pytest.mark.asyncio
async def test_final_retry_release_preserves_cancellation_over_lease_loss():
    runner, _harness, _context = await _runner_fixture(max_attempts=1)
    release_entered = asyncio.Event()
    release_finished = asyncio.Event()
    sleep_delays = []

    async def retryable_failure(*_args, **_kwargs):
        raise UHCFlexPractitionerTransportError(
            "transport_timeout",
            retryable=True,
        )

    async def lease_lost_after_cancellation(*_args, **_kwargs):
        release_entered.set()
        await release_finished.wait()
        raise UHCFlexPractitionerStoreError("lease_lost")

    async def record_sleep(delay_seconds):
        sleep_delays.append(delay_seconds)

    runner.dependencies = replace(
        runner.dependencies,
        fetch=retryable_failure,
        release_work=lease_lost_after_cancellation,
        sleep=record_sleep,
    )
    worker_task = asyncio.create_task(runner.worker(object()))
    await release_entered.wait()
    worker_task.cancel()
    await asyncio.sleep(0)
    release_finished.set()

    with pytest.raises(asyncio.CancelledError):
        await worker_task
    assert sleep_delays == []
