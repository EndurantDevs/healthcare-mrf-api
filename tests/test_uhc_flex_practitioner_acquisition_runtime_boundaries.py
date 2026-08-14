# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure boundaries for exact-cohort Practitioner acquisition."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process import uhc_flex_practitioner_acquisition_runtime as runtime
from process.uhc_flex_practitioner_store import (
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
)
from process.uhc_flex_practitioner_transport import (
    UHCFlexPractitionerTransportError,
)
from tests.uhc_flex_practitioner_acquisition_test_support import (
    AcquisitionHarness,
    enabled_config,
    OPERATION_KEY,
    PROJECTION_DATE,
    query_result_fixture,
)


def _mutated(value, **changes):
    changed = copy.copy(value)
    for field_name, field_value in changes.items():
        object.__setattr__(changed, field_name, field_value)
    return changed


async def _runner_fixture(
    *,
    progress_callback=None,
    npi_count=1,
    **config_changes,
):
    harness = AcquisitionHarness(npi_count=npi_count)
    context = await acquisition._initialize_context(
        operation_key=OPERATION_KEY,
        projection_date=PROJECTION_DATE,
        dependencies=harness.dependencies(),
        database=harness.database,
    )
    runner = runtime._RootRunner(
        context.identity_by_role["baseline"],
        config=enabled_config(concurrency=1, **config_changes),
        dependencies=harness.dependencies(),
        database=harness.database,
        progress_callback=progress_callback,
    )
    return runner, harness, context


@pytest.mark.asyncio
async def test_drain_operation_preserves_or_suppresses_outer_cancellation():
    async def exercise(preserve_cancellation, cancellation_count=1):
        entered = asyncio.Event()
        released = asyncio.Event()

        async def operation():
            entered.set()
            await released.wait()
            return "done"

        task = asyncio.create_task(
            runtime.drain_operation(
                operation(),
                preserve_cancellation=preserve_cancellation,
            )
        )
        await entered.wait()
        for _cancellation_index in range(cancellation_count):
            task.cancel()
            await asyncio.sleep(0)
        released.set()
        return await task

    with pytest.raises(asyncio.CancelledError):
        await exercise(True, cancellation_count=2)
    assert await exercise(False) == "done"

    async def failure():
        raise RuntimeError("bounded")

    with pytest.raises(RuntimeError, match="bounded"):
        await runtime.drain_operation(failure(), preserve_cancellation=False)

    entered = asyncio.Event()
    released = asyncio.Event()

    async def failure_after_cancellation():
        entered.set()
        await released.wait()
        raise UHCFlexPractitionerStoreError("lease_lost")

    task = asyncio.create_task(
        runtime.drain_operation(
            failure_after_cancellation(),
            preserve_cancellation=True,
        )
    )
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_runner_callback_claim_and_retry_failure_boundaries():
    runner, harness, _context = await _runner_fixture()
    await runner.emit("root_started")
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await runner._record_terminal("invalid")

    progress_list = []
    runner.progress_callback = progress_list.append
    await runner.emit("terminal")
    assert progress_list[-1].phase == "terminal"

    async def invalid_claim(*_args, **_kwargs):
        return object()

    runner.dependencies = replace(runner.dependencies, claim_work=invalid_claim)
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await runner.claim()

    async def cancel_claim(*_args, **_kwargs):
        raise asyncio.CancelledError

    runner.dependencies = replace(runner.dependencies, claim_work=cancel_claim)
    with pytest.raises(asyncio.CancelledError):
        await runner.claim()

    async def callback_cancel(_progress):
        raise asyncio.CancelledError

    runner.progress_callback = callback_cancel
    with pytest.raises(asyncio.CancelledError):
        await runner.emit("terminal")
    runner.progress_callback = lambda _progress: (_ for _ in ()).throw(RuntimeError())
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await runner.emit("terminal")

    runner, harness, _context = await _runner_fixture()
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )

    async def release_failure(*_args, **_kwargs):
        raise RuntimeError("release")

    runner.dependencies = replace(runner.dependencies, release_work=release_failure)
    with pytest.raises(RuntimeError, match="release"):
        await runner.release_for_retry(claim)
    await runner.release_for_cancellation(claim)


@pytest.mark.asyncio
async def test_runner_claim_lock_and_invalid_retry_claim():
    runner, _harness, _context = await _runner_fixture()
    await runner._claim_lock.acquire()
    claim_task = asyncio.create_task(runner.claim())
    await asyncio.sleep(0)
    assert not claim_task.done()
    runner._claim_lock.release()
    assert type(await claim_task) is UHCFlexPractitionerWorkClaim

    runner, _harness, _context = await _runner_fixture()

    async def invalid_claim(*_args, **_kwargs):
        return object()

    runner.dependencies = replace(runner.dependencies, claim_work=invalid_claim)
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await runner.claim_retry(1000000004, 0.0)

    runner, _harness, _context = await _runner_fixture()

    async def missing_claim(*_args, **_kwargs):
        return None

    runner.dependencies = replace(runner.dependencies, claim_work=missing_claim)
    assert await runner.claim_retry(1000000004, 0.0) is None


@pytest.mark.asyncio
async def test_released_retry_does_not_block_general_claims():
    runner, harness, _context = await _runner_fixture(npi_count=2)
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


@pytest.mark.asyncio
async def test_runner_cancellation_releases_claim_and_retry_claim():
    for retry_claim in (False, True):
        runner, harness, _context = await _runner_fixture()
        claim = await harness.claim_work(
            runner.identity.acquisition_id,
            lease_seconds=runner.config.lease_seconds,
            database=harness.database,
        )

        async def return_claim(*_args, **_kwargs):
            return claim

        async def cancel_record():
            raise asyncio.CancelledError

        runner.dependencies = replace(runner.dependencies, claim_work=return_claim)
        runner._record_claim = cancel_record
        if retry_claim:
            operation = runner.claim_retry(claim.requested_npi, 0.0)
        else:
            operation = runner.claim()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert not harness.active


@pytest.mark.asyncio
async def test_retry_claim_cancellation_after_claim_releases_exact_lease():
    runner, harness, _context = await _runner_fixture()
    exit_entered = asyncio.Event()

    class CancelableLockExit:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_error):
            exit_entered.set()
            await asyncio.Future()

    runner._claim_lock = CancelableLockExit()
    claim_task = asyncio.create_task(runner.claim_retry(1000000004, 0.0))
    await exit_entered.wait()
    claim_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await claim_task
    assert not harness.active
    assert harness.pending[runner.identity.acquisition_id] == [1000000004]


@pytest.mark.asyncio
async def test_runner_terminal_and_result_cancellation_release_claims():
    for operation_name in ("complete_error", "complete_result"):
        runner, harness, _context = await _runner_fixture()
        claim = await harness.claim_work(
            runner.identity.acquisition_id,
            lease_seconds=runner.config.lease_seconds,
            database=harness.database,
        )

        async def cancel_completion(*_args, **_kwargs):
            raise asyncio.CancelledError

        runner.dependencies = replace(
            runner.dependencies,
            **{operation_name: cancel_completion},
        )
        if operation_name == "complete_error":
            operation = runner.terminal_error(claim, "bounded")
        else:

            async def fetch_result(*_args, **_kwargs):
                return query_result_fixture(claim.requested_npi)

            runner.dependencies = replace(runner.dependencies, fetch=fetch_result)
            operation = runner.process_claim(object(), claim)
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert not harness.active


@pytest.mark.asyncio
async def test_persisted_attempt_count_does_not_exhaust_new_invocation():
    runner, harness, _context = await _runner_fixture(max_attempts=1)
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    claim = _mutated(claim, attempt=2)
    harness.active[(claim.acquisition_id, claim.requested_npi)] = claim
    async with harness.session_scope(1) as session:
        assert await runner.process_claim(session, claim) is None
    assert set(harness.terminal[claim.acquisition_id]) == {claim.requested_npi}


@pytest.mark.asyncio
async def test_final_retryable_lease_is_pending_or_expired_reclaimable():
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

    async def expired_release(*_args, **_kwargs):
        raise UHCFlexPractitionerStoreError("lease_lost")

    runner.dependencies = replace(
        runner.dependencies,
        fetch=retryable_failure,
        release_work=expired_release,
    )
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as caught:
        await runner.process_claim(object(), claim)

    assert caught.value.code == "root_retryable"
    claim_key = (claim.acquisition_id, claim.requested_npi)
    assert harness.active[claim_key] == claim
    harness.active.pop(claim_key)
    harness.pending[claim.acquisition_id].append(claim.requested_npi)
    reclaimed = await harness.claim_work(
        claim.acquisition_id,
        requested_npi=claim.requested_npi,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    assert reclaimed is not None
    assert reclaimed.attempt == claim.attempt + 1
    assert reclaimed.lease_token != claim.lease_token


@pytest.mark.asyncio
async def test_worker_retries_missing_claim_and_run_root_rejects_bad_summary(
    monkeypatch,
):
    runner, harness, _context = await _runner_fixture()
    claim = await harness.claim_work(
        runner.identity.acquisition_id,
        lease_seconds=runner.config.lease_seconds,
        database=harness.database,
    )
    claim_results = iter((claim, None))

    async def next_claim():
        return next(claim_results)

    async def retry_missing(*_args):
        return None

    async def request_retry(*_args):
        return claim.requested_npi, 0.0

    runner.claim = next_claim
    runner.claim_retry = retry_missing
    runner.process_claim = request_retry
    await runner.worker(object())

    async def no_work(_runner, _session):
        return None

    monkeypatch.setattr(runtime._RootRunner, "worker", no_work)

    async def invalid_summary(*_args, **_kwargs):
        return object()

    dependencies = replace(harness.dependencies(), seal_root=invalid_summary)
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError):
        await runtime.run_root(
            runner.identity,
            config=runner.config,
            dependencies=dependencies,
            database=harness.database,
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_run_root_cancels_sibling_after_worker_failure(monkeypatch):
    runner, harness, _context = await _runner_fixture()
    entered = asyncio.Event()
    is_first_call = iter((True, False))

    async def fail_or_block(_runner, _session):
        if next(is_first_call):
            await entered.wait()
            raise RuntimeError("worker")
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(runtime._RootRunner, "worker", fail_or_block)
    with pytest.raises(RuntimeError, match="worker"):
        await runtime.run_root(
            runner.identity,
            config=enabled_config(concurrency=2),
            dependencies=harness.dependencies(),
            database=harness.database,
            progress_callback=None,
        )


def test_retry_delay_rejects_nonfinite_and_boolean_server_hints():
    runner = object.__new__(runtime._RootRunner)
    runner.config = enabled_config(concurrency=1, retry_base_seconds=0.5)
    for value in (True, float("inf"), -2.0):
        error = UHCFlexPractitionerTransportError("transport_timeout", retryable=True)
        error.retry_after_seconds = value
        assert runner.retry_delay(error, 1) == 0.5
