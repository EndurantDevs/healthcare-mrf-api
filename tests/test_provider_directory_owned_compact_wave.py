# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the original compact DML through the actual owned-session bridge."""

import asyncio
import contextlib
import json
import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticResult
from tests.test_provider_directory_owned_evidence_wave import custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave


@pytest.fixture
def compact_wave(owned_wave, monkeypatch):
    state, database, admission, build, _, projection, original_ledger = owned_wave
    object.__setattr__(admission, "geometry", SimpleNamespace(**vars(admission.geometry), bounded_admission=True))
    build.profile_stage = "synthetic_profile"
    build.generation_id = "synthetic_generation"
    build.source_ids = ("synthetic_source",)
    build.retained_source_ids = ("synthetic_retained",)
    monkeypatch.setattr(fhir, "_provider_directory_profile_build_id", lambda _: "synthetic_build")
    status = AsyncMock(wraps=database.scalar)
    monkeypatch.setattr(database, "scalar", status)
    context = SimpleNamespace(
        copy_profiles_sql="INSERT INTO synthetic_profile SELECT 1 ON CONFLICT (npi) DO NOTHING;",
        profile_insert_sql=Mock(return_value="INSERT INTO synthetic_profile SELECT 2 ON CONFLICT (npi) DO NOTHING;"),
        profile_sql_args_by_name={"profile_stage_ref": "synthetic_profile"},
        profile_params_by_name={"source_ids": ["synthetic_source"]},
    )
    batches = [
        fhir._ProviderDirectoryProfileCompactBatch(kind="npi", npi_start=10 + i, npi_end=11 + i) for i in range(2)
    ]
    window = (object(), "profile_stage")
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set(window)
    try:
        yield state, database, admission, build, batches, projection, context, status
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


async def run_wave(fixture, count=1, offset=0):
    _, _, _, build, batches, projection, context, _ = fixture
    coordinates = [(offset + i, batch) for i, batch in enumerate(batches[:count])]
    return await fhir._run_profile_compact_window(
        build, coordinates, context, {number: projection for number, _ in coordinates}
    )


@pytest.mark.asyncio
async def test_original_sql_once_native_result_retained_and_borrowed_pool_one(compact_wave):
    state, _, admission, build, batches, _, context, status = compact_wave
    assert await run_wave(compact_wave, 2) == [1, 1]
    assert state.max_checkedout == 1 and len(state.connections) == 2
    assert status.await_count == context.profile_insert_sql.call_count == 2
    for i, call in enumerate(status.await_args_list):
        assert call.args == (fhir.profile_statement_wal._EXPLAIN + context.profile_insert_sql.return_value,)
        assert call.kwargs == {
            "source_ids": ["synthetic_source"],
            "profile_npi_start": batches[i].npi_start,
            "profile_npi_end": batches[i].npi_end,
        }
    tracker = admission.wal_tracker
    assert tracker.owned_compact_worker_outcomes == {}
    assert len(tracker.owned_compact_wave_outcomes) == 1
    wave = tracker.owned_compact_wave_outcomes[0]
    assert wave["generation_id"] == build.generation_id
    assert wave["build_id"] == "synthetic_build" and wave["coordinate"] == {"batch_start": 0, "batch_end": 2}
    assert wave["expected_workers"] == wave["drained_workers"] == 2
    assert wave["status"] == "committed_measured_mixed_unclassified"
    assert not wave["accounting_authority"] and not wave["reservation_refund"]
    native_wave = tracker.owned_compact_wave_native_outcomes[0]
    assert native_wave["status"] == "complete"
    assert native_wave["build"] is build and native_wave["admission"] is admission
    assert native_wave["window"] is fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    assert not native_wave["accounting_authority"] and not native_wave["reservation_refund"]
    for number, (connection, native_worker, capture) in enumerate(
        zip(state.connections, native_wave["workers"], wave["workers"], strict=True)
    ):
        owner = native_worker["owner"]
        outcome = owner["outcome"]
        assert native_worker["coordinate"] == number
        assert native_worker["task"] is owner["task"] and owner["task"].done()
        assert owner["build"] is build and owner["batch"] is batches[number]
        assert owner["admission"] is admission and owner["window"] is native_wave["window"]
        assert native_wave["coordinates"][number][1] is batches[number]
        assert isinstance(outcome.measurement, BackendWalDiagnosticResult)
        assert outcome.cleanup_complete and outcome.commit_state == "confirmed"
        assert capture["measurement"]["wal_record_bytes"] == str(outcome.measurement.wal_bytes_delta)
        assert capture["wal_classification"] == "mixed_unclassified"
        assert connection.events.index("limits") < connection.events.index("write") < connection.events.index("commit")
        assert connection.events.index("commit") < len(connection.events) - 1 - connection.events[::-1].index("sample")
        assert connection.closed and connection.driver.settings == connection.driver.original


@pytest.mark.asyncio
async def test_copy_statement_and_parameter_binding_remain_original(compact_wave):
    _, _, admission, _, batches, _, context, status = compact_wave
    batches[0] = fhir._ProviderDirectoryProfileCompactBatch(kind="copy")
    assert await run_wave(compact_wave) == [1]
    status.assert_awaited_once_with(
        fhir.profile_statement_wal._EXPLAIN + context.copy_profiles_sql,
        source_ids=["synthetic_source"],
        retained_source_ids=["synthetic_retained"],
        profile_as_of="2026-01-01",
    )
    context.profile_insert_sql.assert_not_called()
    assert admission.wal_tracker.owned_compact_wave_outcomes[0]["workers"][0]["kind"] == "copy"


@pytest.mark.asyncio
async def test_only_current_wave_native_objects_and_summaries_are_retained(compact_wave):
    _, _, admission, _, _, _, _, _ = compact_wave
    assert await run_wave(compact_wave, 2) == [1, 1]
    earlier = admission.wal_tracker.owned_compact_wave_native_outcomes[0]
    earlier_outcome = earlier["workers"][0]["owner"]["outcome"]
    assert await run_wave(compact_wave, 1, offset=2) == [1]
    tracker = admission.wal_tracker
    assert len(tracker.owned_compact_wave_native_outcomes) == len(tracker.owned_compact_wave_outcomes) == 1
    current = tracker.owned_compact_wave_native_outcomes[0]
    assert current is not earlier and len(current["workers"]) == 1
    assert current["workers"][0]["owner"]["outcome"] is not earlier_outcome
    assert current["coordinates"] == ((2, compact_wave[4][0]),)
    assert tracker.owned_compact_wave_outcomes[0]["coordinate"] == {"batch_start": 2, "batch_end": 3}
    assert tracker.owned_compact_worker_outcomes == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["rollback", "overrun", "cancel", "postcommit_sample", "unknown_commit", "restore_failure"]
)
async def test_failure_preserves_original_error_drain_and_commit_truth(compact_wave, mode):
    state, _, admission, _, _, _, _, status = compact_wave
    state.mode = mode
    error_type = {"rollback": ValueError, "overrun": RuntimeError, "cancel": asyncio.CancelledError}.get(
        mode, custody.OwnedWalTransactionError
    )
    with pytest.raises(error_type) as raised:
        await run_wave(compact_wave)
    if mode == "overrun":
        assert str(raised.value) == "provider_directory_profile_compact_projection_exceeded"
    assert status.await_count == 1
    tracker = admission.wal_tracker
    wave = tracker.owned_compact_wave_outcomes[0]
    capture = wave["workers"][0]
    native_wave = tracker.owned_compact_wave_native_outcomes[0]
    outcome = native_wave["workers"][0]["owner"]["outcome"]
    assert native_wave["status"] == "incomplete"
    assert wave["expected_workers"] == wave["drained_workers"] == 1
    assert capture["measurement"] is None and capture["cleanup_complete"] and outcome.cleanup_complete
    assert (
        wave["status"] == "accounting_incomplete"
        and not wave["accounting_authority"]
        and not wave["reservation_refund"]
    )
    if mode in {"postcommit_sample", "restore_failure"}:
        assert capture["committed"] and capture["commit_state"] == "confirmed"
    elif mode == "unknown_commit":
        assert capture["commit_state"] == "attempted" and not capture["committed"]
    else:
        assert capture["commit_state"] == "rolled_back" and "commit" not in state.connections[0].events
    assert tracker.owned_compact_worker_outcomes == {} and all(connection.closed for connection in state.connections)
    if mode == "restore_failure":
        assert state.connections[0].invalidated


@pytest.mark.asyncio
async def test_failure_drains_original_siblings_before_capture_consumption(compact_wave):
    state, _, admission, _, _, _, _, _ = compact_wave
    state.mode = "rollback"
    with pytest.raises(ValueError):
        await run_wave(compact_wave, 2)
    wave = admission.wal_tracker.owned_compact_wave_outcomes[0]
    assert wave["expected_workers"] == wave["drained_workers"] == 2
    assert len(wave["workers"]) == 2 and admission.wal_tracker.owned_compact_worker_outcomes == {}
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_unadmitted_exact_original_sql_int_return_no_owner(compact_wave, monkeypatch):
    _, database, _, build, batches, projection, context, _ = compact_wave
    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", Mock(side_effect=AssertionError("unadmitted owner")))
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(database, "status", status)
    try:
        result = await fhir._execute_provider_directory_profile_compact_batch(
            build,
            batches[0],
            copy_profiles_sql=context.copy_profiles_sql,
            profile_insert_sql=context.profile_insert_sql,
            profile_sql_args_by_name=context.profile_sql_args_by_name,
            profile_params_by_name=context.profile_params_by_name,
            projection=projection,
        )
        assert type(result) is int and result == 1
        status.assert_awaited_once_with(
            context.profile_insert_sql.return_value,
            source_ids=["synthetic_source"],
            profile_npi_start=10,
            profile_npi_end=11,
        )
        custody.registry_owned_wal_transaction.assert_not_called()
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


@pytest.mark.asyncio
async def test_existing_observer_emits_detached_unclassified_current_capture(compact_wave, monkeypatch, caplog):
    monkeypatch.setenv(fhir.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV, "1")
    monkeypatch.setattr(
        fhir,
        "_profile_capacity_observation_sample",
        AsyncMock(return_value={"wal_lsn": "0/10", "wal_bytes": 0, "temp_bytes": 0, "relation_bytes": {"total": 0}}),
    )
    caplog.set_level(logging.INFO, logger=fhir.__name__)
    await run_wave(compact_wave)
    _, _, _, build, _, _, _, _ = compact_wave
    fhir._emit_profile_capacity_observation(
        "compact", {"batch_start": 0, "batch_end": 1}, None, None, 0.0, None, None, None
    )
    observations = [
        json.loads(record.message)
        for record in caplog.records
        if "profile-clone-capacity-observation.v1" in record.message
    ]
    capture = observations[-1]["owned_worker_capture"]
    assert capture["status"] == "committed_measured_mixed_unclassified" and not capture["accounting_authority"]
    assert capture["workers"][0]["measurement"]["units"] == "native_record_bytes"


@pytest.mark.asyncio
async def test_original_body_exception_identity_and_single_capture(compact_wave, monkeypatch):
    _, database, _, _, _, _, _, _ = compact_wave
    failure = ValueError("synthetic original failure instance")
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=failure))
    original = fhir._profile_owned_transaction_capture
    capture = Mock(wraps=original)
    monkeypatch.setattr(fhir, "_profile_owned_transaction_capture", capture)
    with pytest.raises(ValueError) as raised:
        await run_wave(compact_wave)
    assert raised.value is failure
    capture.assert_called_once()


@pytest.mark.asyncio
async def test_reader_candidate_summary_is_matched_without_raw_serialization(compact_wave, caplog):
    _, _, admission, _, _, _, _, _ = compact_wave
    await run_wave(compact_wave)
    summary_by_field = {
        "coordinate": {"batch_start": 0, "batch_end": 1},
        "accounting_authority": False,
        "reservation_refund": False,
        "status": "mixed_unclassified",
    }
    admission.wal_tracker.owned_compact_preflight_outcomes[:] = [summary_by_field]
    caplog.set_level(logging.INFO, logger=fhir.__name__)
    fhir._emit_profile_capacity_observation(
        "compact", summary_by_field["coordinate"], None, None, 0.0, None, None, None
    )
    observation = json.loads(caplog.records[-1].message)
    assert observation["owned_preflight_capture"] == summary_by_field
    assert observation["owned_worker_capture"]["coordinate"] == summary_by_field["coordinate"]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["missing", "duplicate_number", "mismatched_batch", "stray"])
async def test_incomplete_capture_cannot_reach_successful_checkpoint(compact_wave, monkeypatch, corruption):
    _, _, admission, build, batches, projection, context, _ = compact_wave
    original = fhir._execute_provider_directory_profile_compact_batch

    async def worker(*args, **kwargs):
        result = await original(*args, **kwargs)
        entries = admission.wal_tracker.owned_compact_worker_outcomes
        task = asyncio.current_task()
        if corruption == "missing":
            entries.pop(task)
        elif corruption == "mismatched_batch":
            entries[task]["batch"] = object()
        elif corruption == "stray":
            entries[object()] = entries[task]
        return result

    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_compact_batch", worker)
    coordinates = [(0, batches[0]), (0 if corruption == "duplicate_number" else 1, batches[1])]
    with pytest.raises(RuntimeError, match="compact_owned_capture_incomplete"):
        await fhir._run_profile_compact_window(build, coordinates, context, {0: projection, 1: projection})
    wave = admission.wal_tracker.owned_compact_wave_outcomes[0]
    assert wave["status"] == "accounting_incomplete" and wave["drained_workers"] == 2
    assert not wave["accounting_authority"] and not wave["reservation_refund"]
    connection_count = len(compact_wave[0].connections)
    with pytest.raises(RuntimeError, match="compact_owned_capture_unconsumed"):
        await run_wave(compact_wave)
    assert len(compact_wave[0].connections) == connection_count
    if corruption == "stray":
        assert wave["unconsumed_workers"] > 0
        assert admission.wal_tracker.owned_compact_worker_outcomes
        with pytest.raises(RuntimeError, match="compact_owned_capture_unconsumed"):
            await run_wave(compact_wave)


@pytest.mark.asyncio
@pytest.mark.parametrize("geometry", [None, SimpleNamespace(), SimpleNamespace(bounded_admission=False)])
async def test_legacy_admitted_path_needs_no_retained_capture(compact_wave, monkeypatch, geometry):
    _, database, admission, build, batches, projection, context, _ = compact_wave
    monkeypatch.setattr(
        fhir, "_provider_directory_profile_capacity_admission", lambda: replace(admission, geometry=geometry)
    )
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", Mock(side_effect=AssertionError("legacy owner")))
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(database, "status", status)

    @contextlib.asynccontextmanager
    async def original_capacity_transaction():
        yield

    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", original_capacity_transaction)
    assert await fhir._run_profile_compact_window(build, [(0, batches[0])], context, {0: projection}) == [1]
    status.assert_awaited_once()
    custody.registry_owned_wal_transaction.assert_not_called()
    assert admission.wal_tracker.owned_compact_wave_outcomes == []


@pytest.mark.asyncio
async def test_consumer_failure_does_not_mask_original_worker_error(compact_wave, monkeypatch):
    _, database, _, _, _, _, _, _ = compact_wave
    failure = ValueError("synthetic original body error")
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=failure))
    monkeypatch.setattr(
        fhir,
        "_consume_profile_compact_worker_outcomes",
        Mock(side_effect=RuntimeError("synthetic diagnostic consumer failure")),
    )
    with pytest.raises(ValueError) as raised:
        await run_wave(compact_wave)
    assert raised.value is failure


@pytest.mark.asyncio
async def test_legacy_projection_rejection_rolls_back_before_transaction_exit(compact_wave, monkeypatch):
    _, database, admission, build, batches, projection, context, _ = compact_wave
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_capacity_admission",
        lambda: replace(admission, geometry=SimpleNamespace(bounded_admission=False)),
    )
    monkeypatch.setattr(database, "status", AsyncMock(return_value=projection.projected_rows + 1))
    events = []

    @contextlib.asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", transaction)
    with pytest.raises(RuntimeError, match="compact_projection_exceeded"):
        await fhir._execute_provider_directory_profile_compact_batch(
            build,
            batches[0],
            copy_profiles_sql=context.copy_profiles_sql,
            profile_insert_sql=context.profile_insert_sql,
            profile_sql_args_by_name=context.profile_sql_args_by_name,
            profile_params_by_name=context.profile_params_by_name,
            projection=projection,
        )
    assert events == ["begin", "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["stale_window", "equal_window", "wrong_admission", "wrong_task"])
async def test_native_owner_identity_mismatch_stays_incomplete(compact_wave, monkeypatch, corruption):
    _, _, admission, build, batches, _, _, _ = compact_wave
    original = fhir._execute_provider_directory_profile_compact_batch
    captured_owners = []

    async def worker(*args, **kwargs):
        result = await original(*args, **kwargs)
        owner = admission.wal_tracker.owned_compact_worker_outcomes[asyncio.current_task()]
        if corruption == "stale_window":
            owner["window"] = (object(), "profile_stage")
        elif corruption == "equal_window":
            window = owner["window"]
            owner["window"] = (window[0], window[1])
            assert owner["window"] == window and owner["window"] is not window
        elif corruption == "wrong_admission":
            owner["admission"] = replace(admission)
        else:
            owner["task"] = object()
        captured_owners.append(owner)
        return result

    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_compact_batch", worker)
    with pytest.raises(RuntimeError, match="compact_owned_capture_incomplete"):
        await run_wave(compact_wave)
    tracker = admission.wal_tracker
    native_wave = tracker.owned_compact_wave_native_outcomes[0]
    native_worker = native_wave["workers"][0]
    assert native_wave["status"] == "incomplete"
    assert native_wave["build"] is build and native_wave["admission"] is admission
    assert native_wave["coordinates"] == ((0, batches[0]),)
    assert native_worker["owner"] is captured_owners[0]
    outcome = native_worker["owner"]["outcome"]
    assert isinstance(outcome, custody.OwnedWalTransaction)
    assert isinstance(outcome.measurement, BackendWalDiagnosticResult)
    assert outcome.is_committed and outcome.cleanup_complete
    assert tracker.owned_compact_wave_outcomes[0]["matched_workers"] == 0
    assert not native_wave["accounting_authority"] and not native_wave["reservation_refund"]
    assert tracker.owned_compact_worker_outcomes[native_worker["task"]] is captured_owners[0]
    with pytest.raises(RuntimeError, match="compact_owned_capture_unconsumed"):
        await run_wave(compact_wave)
    assert tracker.owned_compact_wave_native_outcomes[0] is native_wave


@pytest.mark.asyncio
@pytest.mark.parametrize("swap_batches", [False, True])
async def test_native_wave_keeps_exact_nonconsecutive_coordinates(compact_wave, monkeypatch, swap_batches):
    _, _, admission, build, batches, projection, context, _ = compact_wave
    coordinates = [(7, batches[0]), (13, batches[1])]
    original = fhir._consume_profile_compact_worker_outcomes
    captured_owners = []

    def consume(current_admission, current_build, tasks, current_coordinates):
        captured_owners.extend(current_admission.wal_tracker.owned_compact_worker_outcomes[task] for task in tasks)
        if swap_batches:
            current_coordinates = [(7, batches[1]), (13, batches[0])]
        original(current_admission, current_build, tasks, current_coordinates)

    monkeypatch.setattr(fhir, "_consume_profile_compact_worker_outcomes", consume)
    if swap_batches:
        with pytest.raises(RuntimeError, match="compact_owned_capture_incomplete"):
            await fhir._run_profile_compact_window(build, coordinates, context, {7: projection, 13: projection})
    else:
        assert await fhir._run_profile_compact_window(build, coordinates, context, {7: projection, 13: projection}) == [
            1,
            1,
        ]
    native_wave = admission.wal_tracker.owned_compact_wave_native_outcomes[0]
    assert native_wave["status"] == ("incomplete" if swap_batches else "complete")
    assert [worker["coordinate"] for worker in native_wave["workers"]] == [7, 13]
    for index, worker in enumerate(native_wave["workers"]):
        assert worker["owner"] is captured_owners[index]
        assert worker["owner"]["batch"] is batches[index]
        assert native_wave["coordinates"][index][1] is batches[1 - index if swap_batches else index]
        assert worker["task"] is captured_owners[index]["task"] and worker["task"].done()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["rollback", "cancel"])
@pytest.mark.parametrize("diagnostic", ["missing", "malformed", "malformed_type", "raises", "always_raises"])
async def test_diagnostic_failure_preserves_first_error_and_native_owner(compact_wave, monkeypatch, mode, diagnostic):
    state, _, admission, _, _, _, _, _ = compact_wave
    state.mode = mode
    original = fhir._profile_owned_transaction_capture

    def capture(outcome, failure):
        if diagnostic == "always_raises":
            raise RuntimeError("synthetic missing-outcome serialization failure")
        if outcome is None:
            return original(outcome, failure)
        if diagnostic == "missing":
            return None
        if diagnostic == "malformed":
            return {"measurement_status": "incomplete"}
        if diagnostic == "malformed_type":
            return "synthetic malformed diagnostic"
        raise RuntimeError("synthetic diagnostic failure")

    monkeypatch.setattr(fhir, "_profile_owned_transaction_capture", capture)
    error_type = ValueError if mode == "rollback" else asyncio.CancelledError
    with pytest.raises(error_type) as raised:
        await run_wave(compact_wave)
    if mode == "rollback":
        assert str(raised.value) == "synthetic statement failure"
    native_wave = admission.wal_tracker.owned_compact_wave_native_outcomes[0]
    outcome = native_wave["workers"][0]["owner"]["outcome"]
    assert isinstance(outcome, custody.OwnedWalTransaction)
    assert outcome.commit_state == "rolled_back" and outcome.cleanup_complete
    assert native_wave["status"] == "incomplete"
    assert not native_wave["accounting_authority"] and not native_wave["reservation_refund"]
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", ["missing", "wrong_type", "commit", "cleanup", "status", "measurement", "identity"]
)
async def test_forged_complete_diagnostic_cannot_replace_native_completion(compact_wave, monkeypatch, corruption):
    _, _, admission, _, _, _, _, _ = compact_wave
    original = fhir._execute_provider_directory_profile_compact_batch
    captured_owners = []

    async def worker(*args, **kwargs):
        result = await original(*args, **kwargs)
        owner = admission.wal_tracker.owned_compact_worker_outcomes[asyncio.current_task()]
        outcome = owner["outcome"]
        assert isinstance(outcome, custody.OwnedWalTransaction)
        assert isinstance(outcome.measurement, BackendWalDiagnosticResult)
        owner["capture"] = {"measurement_status": "complete"}
        if corruption in {"missing", "wrong_type"}:
            owner["outcome"] = None if corruption == "missing" else SimpleNamespace(**vars(outcome), is_committed=True)
        elif corruption == "identity":
            final = outcome.measurement.final
            outcome.measurement = replace(
                outcome.measurement, final=replace(final, identity=replace(final.identity, pid=9999))
            )
        else:
            attribute, replacement = {
                "commit": ("commit_state", "attempted"),
                "cleanup": ("cleanup_complete", False),
                "status": ("status", "committed_accounting_incomplete"),
                "measurement": ("measurement", None),
            }[corruption]
            setattr(outcome, attribute, replacement)
        captured_owners.append(owner)
        return result

    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_compact_batch", worker)
    with pytest.raises(RuntimeError, match="compact_owned_capture_incomplete"):
        await run_wave(compact_wave)
    tracker = admission.wal_tracker
    native_wave = tracker.owned_compact_wave_native_outcomes[0]
    assert native_wave["status"] == "incomplete"
    assert native_wave["workers"][0]["owner"] is captured_owners[0]
    summary = tracker.owned_compact_wave_outcomes[0]
    assert summary["status"] == "accounting_incomplete"
    assert summary["workers"][0]["measurement_status"] == "incomplete"
    assert summary["workers"][0]["measurement"] is None
    assert summary["workers"][0]["status"] in {"accounting_incomplete", "committed_accounting_incomplete"}
    assert captured_owners[0]["capture"] == {"measurement_status": "complete"}
    with pytest.raises(RuntimeError, match="compact_owned_capture_unconsumed"):
        await run_wave(compact_wave)
    assert tracker.owned_compact_wave_native_outcomes[0] is native_wave
