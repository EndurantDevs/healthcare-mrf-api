# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Worker and retry behavior for the exact-cohort Practitioner orchestrator."""

from __future__ import annotations

import asyncio

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process.uhc_flex_practitioner_store import (
    UHCFlexPractitionerStoreError,
)
from process.uhc_flex_practitioner_transport import (
    UHCFlexPractitionerTransportError,
)
from tests.uhc_flex_practitioner_acquisition_test_support import (
    acquire_with_harness,
    AcquisitionHarness,
    enabled_config,
    MEMBER_NPIS,
)


@pytest.mark.asyncio
async def test_deterministic_identities_resume_and_admission_ordering():
    harness = AcquisitionHarness()

    first = await acquire_with_harness(harness)
    fetch_count = harness.fetch_count
    second = await acquire_with_harness(harness)

    assert first.dataset_intent_id == second.dataset_intent_id
    assert first.baseline.acquisition_id == second.baseline.acquisition_id
    assert first.candidate.acquisition_id == second.candidate.acquisition_id
    assert first.baseline.run_id == second.baseline.run_id
    assert first.candidate.run_id == second.candidate.run_id
    assert first.admission_id == second.admission_id
    assert harness.fetch_count == fetch_count
    assert harness.events.index("initialize:baseline") < harness.events.index(
        "initialize:candidate"
    )
    assert harness.events.index("seal:baseline") < harness.events.index(
        "session_enter:candidate"
    )
    assert harness.events.index("seal:candidate") < harness.events.index("admit")
    assert repr(first) == (
        "<uhc-flex-practitioner-acquisition-receipt "
        "expected=2 matched=1 unmatched=1>"
    )
    assert all(
        str(npi) not in repr(progress)
        for progress in harness.progress
        for npi in harness.npis
    )


@pytest.mark.asyncio
async def test_runtime_concurrency_and_connection_limit_are_hard_bounded():
    harness = AcquisitionHarness(npi_count=8)
    harness.fetch_barrier_target = 3

    receipt = await acquire_with_harness(
        harness,
        config=enabled_config(concurrency=3),
    )

    assert receipt.expected_npi_count == 8
    assert harness.maximum_active_fetches == 3
    assert [session.connection_limit for session in harness.sessions] == [3, 3]
    with pytest.raises(ValueError):
        acquisition.UHCFlexPractitionerAcquisitionConfig(
            enabled=True,
            concurrency=(
                acquisition.UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY
                + 1
            ),
        )


@pytest.mark.asyncio
async def test_transient_failure_releases_before_bounded_retry_after_sleep():
    harness = AcquisitionHarness()
    transient_error = UHCFlexPractitionerTransportError(
        "http_transient",
        retryable=True,
        retry_after_seconds=900,
    )
    transient_error.retry_after_seconds = 900.0
    harness.fetch_failures[("baseline", MEMBER_NPIS[0], 1)] = transient_error

    receipt = await acquire_with_harness(
        harness,
        config=enabled_config(concurrency=1, retry_base_seconds=0.25),
    )

    assert receipt.baseline.matched_count == 1
    assert harness.sleep_delays == [60.0]
    release_index = harness.events.index("release:baseline")
    assert release_index < harness.events.index("sleep")
    assert harness.attempts[(receipt.baseline.acquisition_id, MEMBER_NPIS[0])] == 2
    assert any(progress.retry_count == 1 for progress in harness.progress)


@pytest.mark.asyncio
async def test_nonretryable_and_exhausted_transport_failures_are_terminal_safe():
    for planned_error, expected_code, max_attempts in (
        (
            UHCFlexPractitionerTransportError("http_terminal"),
            "http_terminal",
            3,
        ),
        (
            UHCFlexPractitionerTransportError(
                "transport_timeout",
                retryable=True,
            ),
            "retry_exhausted",
            1,
        ),
        (
            RuntimeError(f"sensitive failure for {MEMBER_NPIS[0]}"),
            "transport_failure",
            3,
        ),
    ):
        harness = AcquisitionHarness()
        harness.fetch_failures[("baseline", MEMBER_NPIS[0], 1)] = planned_error

        with pytest.raises(
            acquisition.UHCFlexPractitionerAcquisitionError
        ) as error_info:
            await acquire_with_harness(
                harness,
                config=enabled_config(concurrency=1, max_attempts=max_attempts),
            )

        assert error_info.value.code == "root_unsealable"
        assert "sensitive" not in str(error_info.value)
        assert str(MEMBER_NPIS[0]) not in str(error_info.value)
        assert f"error:baseline:{expected_code}" in harness.events
        assert "session_enter:candidate" not in harness.events
        assert "admit" not in harness.events
        baseline_id = next(iter(harness.identities))
        assert any(
            terminal[0] == "error"
            for terminal in harness.terminal[baseline_id].values()
        )
        with pytest.raises(UHCFlexPractitionerStoreError):
            await harness.seal_root(
                harness.identities[baseline_id],
                database=None,
            )


@pytest.mark.asyncio
async def test_cancellation_drains_fenced_release_and_root_resumes():
    harness = AcquisitionHarness()
    harness.block_fetch = True
    acquisition_task = asyncio.create_task(
        acquire_with_harness(harness, config=enabled_config(concurrency=1))
    )
    await harness.fetch_entered.wait()

    acquisition_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquisition_task

    baseline_id = next(
        identity.acquisition_id
        for identity in harness.identities.values()
        if identity.acquisition_role == "baseline"
    )
    assert "release:baseline" in harness.events
    assert not harness.active
    assert harness.pending[baseline_id] == list(harness.npis)
    assert "session_enter:candidate" not in harness.events

    harness.block_fetch = False
    harness.fetch_entered.clear()
    receipt = await acquire_with_harness(
        harness,
        config=enabled_config(concurrency=1),
    )
    assert receipt.expected_npi_count == len(harness.npis)
    assert harness.attempts[(baseline_id, MEMBER_NPIS[0])] == 2
