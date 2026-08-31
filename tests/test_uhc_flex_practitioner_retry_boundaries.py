# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persisted and per-invocation retry ceilings for Flex acquisition."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process import uhc_flex_practitioner_acquisition_runtime as runtime
from process.uhc_flex_practitioner_transport import (
    UHCFlexPractitionerTransportError,
)
from tests.test_uhc_flex_practitioner_acquisition_runtime_boundaries import (
    _mutated,
    _runner_fixture,
)


@pytest.mark.asyncio
async def test_success_at_persisted_attempt_limit_is_retained():
    runner, harness, _context = await _runner_fixture(max_attempts=1)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    claim = _mutated(
        claim,
        attempt=runtime.UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS,
    )
    harness.active[(claim.acquisition_id, claim.requested_npi)] = claim
    async with harness.session_scope(1) as session:
        assert await runner.process_claim(session, claim) is None
    assert harness.terminal[claim.acquisition_id][claim.requested_npi][0] == "matched"


@pytest.mark.asyncio
async def test_retryable_failure_below_persisted_attempt_limit_retries():
    runner, harness, _context = await _runner_fixture(max_attempts=2)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )

    async def retryable_failure(*_args, **_kwargs):
        raise UHCFlexPractitionerTransportError(
            "transport_timeout",
            retryable=True,
        )

    runner.dependencies = replace(runner.dependencies, fetch=retryable_failure)
    assert await runner.process_claim(object(), claim) == (
        claim.requested_npi,
        runner.retry_delay(
            UHCFlexPractitionerTransportError(
                "transport_timeout",
                retryable=True,
            ),
            1,
        ),
    )
    assert harness.pending[claim.acquisition_id] == [claim.requested_npi]
    assert not harness.terminal[claim.acquisition_id]


@pytest.mark.asyncio
async def test_retryable_failure_at_persisted_attempt_limit_is_terminal():
    runner, harness, _context = await _runner_fixture(max_attempts=1)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    claim = _mutated(
        claim,
        attempt=runtime.UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS,
    )
    harness.active[(claim.acquisition_id, claim.requested_npi)] = claim

    async def retryable_failure(*_args, **_kwargs):
        raise UHCFlexPractitionerTransportError(
            "transport_timeout",
            retryable=True,
        )

    runner.dependencies = replace(runner.dependencies, fetch=retryable_failure)
    assert await runner.process_claim(object(), claim) is None
    assert harness.terminal[claim.acquisition_id][claim.requested_npi] == (
        "error",
        None,
        0,
        "retry_exhausted_transport",
    )
    assert claim.requested_npi not in harness.pending[claim.acquisition_id]


@pytest.mark.asyncio
async def test_already_exhausted_claim_is_terminal_without_another_fetch():
    runner, harness, _context = await _runner_fixture(max_attempts=1)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    claim = _mutated(
        claim,
        attempt=runtime.UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS + 1,
    )
    harness.active[(claim.acquisition_id, claim.requested_npi)] = claim
    fetch = AsyncMock()
    runner.dependencies = replace(runner.dependencies, fetch=fetch)

    assert await runner.process_claim(object(), claim) is None
    fetch.assert_not_awaited()
    assert harness.terminal[claim.acquisition_id][claim.requested_npi] == (
        "error",
        None,
        0,
        "retry_exhausted_transport",
    )


@pytest.mark.asyncio
async def test_invocation_limit_releases_before_persisted_limit():
    runner, harness, _context = await _runner_fixture(max_attempts=1)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )

    async def retryable_failure(*_args, **_kwargs):
        raise UHCFlexPractitionerTransportError(
            "transport_timeout",
            retryable=True,
        )

    runner.dependencies = replace(runner.dependencies, fetch=retryable_failure)
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as caught:
        await runner.process_claim(object(), claim)

    assert caught.value.code == "root_retryable"
    assert harness.pending[claim.acquisition_id] == [claim.requested_npi]
    assert not harness.terminal[claim.acquisition_id]
