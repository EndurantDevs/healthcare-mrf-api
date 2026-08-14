# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Worker and retry behavior for the exact-cohort Practitioner orchestrator."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import replace

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process.uhc_flex_practitioner_store import (
    UHCFlexPractitionerStoreError,
)
from process.uhc_flex_practitioner_single_root_contract import (
    build_single_root_admission,
    UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY,
)
from process.uhc_flex_practitioner_twin_identity import (
    build_uhc_flex_practitioner_dataset_intent_id,
)
from process.uhc_flex_practitioner_transport import (
    UHCFlexPractitionerTransportError,
)
from tests.uhc_flex_practitioner_acquisition_test_support import (
    acquire_with_harness,
    AcquisitionHarness,
    enabled_config,
    MEMBER_NPIS,
    OPERATION_KEY,
    PROJECTION_DATE,
    query_result_fixture,
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
async def test_reviewed_single_root_runs_only_one_distinct_candidate():
    harness = AcquisitionHarness()
    harness.session_serial = 1

    async def admit_single_root(candidate_acquisition_id, **coordinates):
        harness.events.append("admit_single_root")
        return build_single_root_admission(
            harness.sealed_root(candidate_acquisition_id),
            semantic_projection_as_of=coordinates["semantic_projection_as_of"],
            operation_key=coordinates["operation_key"],
            admitted_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )

    dependencies = replace(
        harness.dependencies(),
        admit_single_root=admit_single_root,
    )
    receipt = await acquisition.acquire_uhc_flex_single_root(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of=PROJECTION_DATE,
        config=enabled_config(concurrency=2),
        database=harness.database,
        dependencies=dependencies,
        progress_callback=harness.progress_callback,
    )

    assert {identity.acquisition_role for identity in harness.identities.values()} == {
        "candidate"
    }
    assert "initialize:baseline" not in harness.events
    assert "admit" not in harness.events
    assert harness.events.index("seal:candidate") < harness.events.index(
        "admit_single_root"
    )
    assert receipt.reviewed_root_policy_json == (
        UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document()
    )
    assert receipt.dataset_intent_id != build_uhc_flex_practitioner_dataset_intent_id(
        receipt.cohort_id,
        PROJECTION_DATE,
        OPERATION_KEY,
    )


@pytest.mark.asyncio
async def test_reviewed_single_root_resumes_retryable_work_without_reinitializing():
    harness = AcquisitionHarness()
    harness.session_serial = 1
    monotonic = iter((0.0, 10.0, 20.0, 100.0, 110.0, 120.0, 130.0)).__next__
    for call_number in (1, 2):
        harness.fetch_failures[("candidate", MEMBER_NPIS[0], call_number)] = (
            UHCFlexPractitionerTransportError("transport_timeout", retryable=True)
        )

    async def admit_single_root(candidate_acquisition_id, **coordinates):
        return build_single_root_admission(
            harness.sealed_root(candidate_acquisition_id),
            semantic_projection_as_of=coordinates["semantic_projection_as_of"],
            operation_key=coordinates["operation_key"],
            admitted_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )

    receipt = await acquisition.acquire_uhc_flex_single_root(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of=PROJECTION_DATE,
        config=enabled_config(concurrency=1, max_attempts=2),
        database=harness.database,
        dependencies=replace(
            harness.dependencies(),
            admit_single_root=admit_single_root,
            monotonic=monotonic,
        ),
    )

    assert harness.events.count("initialize:candidate") == 1
    assert sum(event.startswith("session_exit:") for event in harness.events) == 1
    assert harness.sleep_delays == [1.0, 60.0]
    assert harness.attempts[(receipt.candidate.acquisition_id, MEMBER_NPIS[0])] == 3
    assert (receipt.candidate.elapsed_seconds, receipt.elapsed_seconds) == (100.0, 120.0)


@pytest.mark.asyncio
async def test_reviewed_single_root_cancellation_interrupts_resume_cooldown():
    harness = AcquisitionHarness(npi_count=1)
    harness.session_serial = 1
    harness.fetch_failures[("candidate", MEMBER_NPIS[0], 1)] = (
        UHCFlexPractitionerTransportError("transport_timeout", retryable=True)
    )
    cooldown_started = asyncio.Event()

    async def sleep(delay_seconds):
        harness.sleep_delays.append(delay_seconds)
        cooldown_started.set()
        await asyncio.Future()

    acquisition_task = asyncio.create_task(
        acquisition.acquire_uhc_flex_single_root(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of=PROJECTION_DATE,
            config=enabled_config(concurrency=1, max_attempts=1),
            database=harness.database,
            dependencies=replace(harness.dependencies(), sleep=sleep),
        )
    )
    await cooldown_started.wait()
    acquisition_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await acquisition_task

    acquisition_id = next(iter(harness.identities))
    assert harness.events.count("initialize:candidate") == 1
    assert len(harness.sessions) == 1
    assert harness.sleep_delays == [60.0]
    assert harness.pending[acquisition_id] == [MEMBER_NPIS[0]]
    assert not harness.active
    assert not harness.admissions


@pytest.mark.asyncio
async def test_reviewed_single_root_keeps_nonretryable_failures_terminal():
    harness = AcquisitionHarness(npi_count=1)
    harness.session_serial = 1
    harness.fetch_failures[("candidate", MEMBER_NPIS[0], 1)] = (
        UHCFlexPractitionerTransportError(
            "response_validation",
            validation_code="cross_npi",
        )
    )

    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as error_info:
        await acquisition.acquire_uhc_flex_single_root(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of=PROJECTION_DATE,
            config=enabled_config(concurrency=1),
            database=harness.database,
            dependencies=harness.dependencies(),
        )

    acquisition_id = next(iter(harness.identities))
    assert error_info.value.code == "root_unsealable"
    assert harness.events.count("initialize:candidate") == 1
    assert len(harness.sessions) == 1
    assert harness.sleep_delays == []
    assert harness.terminal[acquisition_id][MEMBER_NPIS[0]][0] == "error"


@pytest.mark.asyncio
async def test_reviewed_single_root_uses_the_production_admitter(monkeypatch):
    from process import uhc_flex_practitioner_twin_store as twin_store

    harness = AcquisitionHarness()
    harness.session_serial = 1

    async def admit(candidate_acquisition_id, **coordinates):
        return build_single_root_admission(
            harness.sealed_root(candidate_acquisition_id),
            semantic_projection_as_of=coordinates["semantic_projection_as_of"],
            operation_key=coordinates["operation_key"],
            admitted_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        )

    monkeypatch.setattr(
        twin_store, "admit_uhc_flex_practitioner_single_root", admit
    )
    receipt = await acquisition.acquire_uhc_flex_single_root(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of=PROJECTION_DATE,
        config=enabled_config(concurrency=2),
        database=harness.database,
        dependencies=harness.dependencies(),
    )

    assert receipt.admission_id.startswith("pdufpad_")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("initialize", "admission"))
async def test_reviewed_single_root_rejects_impossible_runtime_state(failure):
    harness = AcquisitionHarness()
    harness.session_serial = 1

    async def invalid_initialize(*_args, **_kwargs):
        return 2

    async def invalid_admission(*_args, **_kwargs):
        return object()

    dependencies = harness.dependencies()
    dependencies = replace(
        dependencies,
        initialize_root=(
            invalid_initialize
            if failure == "initialize"
            else dependencies.initialize_root
        ),
        admit_single_root=(
            invalid_admission if failure == "admission" else None
        ),
    )
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as caught:
        await acquisition.acquire_uhc_flex_single_root(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of=PROJECTION_DATE,
            config=enabled_config(concurrency=2),
            database=harness.database,
            dependencies=dependencies,
        )

    assert caught.value.code == "state"


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
async def test_final_retryable_failure_resumes_same_worker_and_root():
    harness = AcquisitionHarness()
    retryable_failure = UHCFlexPractitionerTransportError("transport_timeout", retryable=True)
    for call_number in (1, 2):
        harness.fetch_failures[("baseline", MEMBER_NPIS[1], call_number)] = retryable_failure

    receipt = await acquire_with_harness(
        harness,
        config=enabled_config(concurrency=1, max_attempts=2),
    )
    baseline_id = receipt.baseline.acquisition_id
    assert not harness.pending[baseline_id]
    assert not harness.active
    assert set(harness.terminal[baseline_id]) == set(harness.npis)
    assert harness.fetch_calls[("baseline", MEMBER_NPIS[0])] == 1
    assert harness.attempts[(baseline_id, MEMBER_NPIS[1])] == 3
    assert harness.sleep_delays == [1.0, 60.0]


@pytest.mark.asyncio
async def test_final_retryable_cools_only_its_worker_while_sibling_finishes_work():
    """Keep fresh work moving while one worker owns the bounded cooldown."""
    harness = AcquisitionHarness(npi_count=3)
    retry_npi, sibling_npi, remaining_npi = harness.npis
    sibling_fetch_entered = asyncio.Event()
    cooldown_started = asyncio.Event()
    cooldown_finished = asyncio.Event()
    fresh_work_finished = asyncio.Event()
    retry_call_list = []

    async def fetch(_session, requested_npi):
        if requested_npi == retry_npi:
            retry_call_list.append(requested_npi)
            if len(retry_call_list) == 1:
                await sibling_fetch_entered.wait()
                raise UHCFlexPractitionerTransportError("transport_timeout", retryable=True)
        elif requested_npi == sibling_npi:
            sibling_fetch_entered.set()
            await cooldown_started.wait()
        return query_result_fixture(requested_npi)

    async def sleep(delay_seconds):
        harness.sleep_delays.append(delay_seconds)
        cooldown_started.set()
        await cooldown_finished.wait()

    async def complete_result(claim, query_result, *, database):
        await harness.complete_result(claim, query_result, database=database)
        terminal = harness.terminal[claim.acquisition_id]
        if sibling_npi in terminal and remaining_npi in terminal:
            fresh_work_finished.set()

    dependencies = replace(
        harness.dependencies(), fetch=fetch, sleep=sleep, complete_result=complete_result
    )
    context = await acquisition._initialize_context(
        operation_key=OPERATION_KEY,
        projection_date=PROJECTION_DATE,
        dependencies=dependencies,
        database=harness.database,
    )
    root_task = asyncio.create_task(
        acquisition._run_root(
            context.identity_by_role["baseline"],
            config=enabled_config(concurrency=2, max_attempts=1),
            dependencies=dependencies,
            database=harness.database,
            progress_callback=None,
        )
    )

    await asyncio.wait_for(cooldown_started.wait(), timeout=0.5)
    await asyncio.wait_for(fresh_work_finished.wait(), timeout=0.5)
    assert not root_task.done()
    assert not any(event.startswith("error:") for event in harness.events)
    cooldown_finished.set()
    summary, _elapsed_seconds = await asyncio.wait_for(root_task, timeout=0.5)
    assert summary.error_count == 0
    assert len(retry_call_list) == 2
    assert harness.sleep_delays == [60.0]


@pytest.mark.asyncio
async def test_nonretryable_transport_failures_are_terminal_safe():
    for planned_error, expected_code, max_attempts in (
        (
            UHCFlexPractitionerTransportError("http_terminal"),
            "http_terminal",
            3,
        ),
        (
            UHCFlexPractitionerTransportError("response_validation", validation_code="cross_npi"),
            "response_validation_cross_npi",
            3,
        ),
        (
            UHCFlexPractitionerTransportError("response_validation", validation_code="secret"),
            "response_validation",
            3,
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
