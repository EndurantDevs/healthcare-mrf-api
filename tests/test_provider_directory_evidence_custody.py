# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep original evidence-worker objects without granting WAL accounting authority."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_owned_evidence_wave import custody, fhir, run_wave
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave


@pytest.fixture
def bounded_wave(owned_wave):
    admission = owned_wave[2]
    object.__setattr__(admission, "geometry", SimpleNamespace(**vars(admission.geometry), bounded_admission=True))
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), "evidence_stage"))
    try:
        yield owned_wave
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


@pytest.mark.asyncio
async def test_original_outcome_witness_and_current_wave_identity(bounded_wave, monkeypatch):
    state, _, admission, build, batches, _, _ = bounded_wave
    originals = []
    record_outcome = fhir._record_profile_evidence_worker_outcome

    def retain(*args, **kwargs):
        originals.append((args[2], args[4]))
        record_outcome(*args, **kwargs)

    monkeypatch.setattr(fhir, "_record_profile_evidence_worker_outcome", retain)
    assert await run_wave(bounded_wave, 2) == [1, 1]
    tracker = admission.wal_tracker
    assert not tracker.owned_evidence_worker_outcomes
    native = tracker.owned_evidence_wave_native_outcomes[0]
    assert native["status"] == "complete" and len(native["workers"]) == 2
    assert native["build"] is build and native["admission"] is admission
    assert native["window"] is fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    assert native["coordinates"] == tuple(enumerate(batches))
    assert not native["accounting_authority"] and not native["reservation_refund"]
    for number, worker in enumerate(native["workers"]):
        owner = worker["owner"]
        assert worker["coordinate"] == owner["coordinate"] == number and worker["task"] is owner["task"]
        assert owner["task"].done() and owner["build"] is build and owner["batch"] is batches[number]
        assert owner["admission"] is admission and owner["window"] is native["window"]
        assert owner["outcome"] is originals[number][0]
        assert owner["statement_witness"] is originals[number][1]
        assert isinstance(owner["outcome"], custody.OwnedWalTransaction)
        assert isinstance(owner["statement_witness"], fhir.profile_statement_wal.TargetStatementWalWitness)
        assert owner["statement_witness"].affected_rows == 1
    assert state.max_checkedout == 1
    assert await run_wave(bounded_wave) == [1]
    assert len(tracker.owned_evidence_wave_native_outcomes) == 1
    assert len(tracker.owned_evidence_wave_native_outcomes[0]["workers"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["task", "admission", "window", "build", "batch", "coordinate"])
async def test_misbound_worker_stays_held_and_prevents_reuse(bounded_wave, monkeypatch, field):
    admission = bounded_wave[2]
    original = fhir._record_profile_evidence_worker_outcome

    def record(*args, **kwargs):
        original(*args, **kwargs)
        admission.wal_tracker.owned_evidence_worker_outcomes[args[1]][field] = object()

    monkeypatch.setattr(fhir, "_record_profile_evidence_worker_outcome", record)
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await run_wave(bounded_wave)
    tracker = admission.wal_tracker
    held_by_task = dict(tracker.owned_evidence_worker_outcomes)
    assert len(held_by_task) == 1 and tracker.owned_evidence_wave_native_outcomes[0]["status"] == "incomplete"
    with pytest.raises(RuntimeError, match="owned_capture_unconsumed"):
        await run_wave(bounded_wave)
    assert tracker.owned_evidence_worker_outcomes == held_by_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["missing", "commit", "cleanup", "measurement", "identity", "witness", "witness_exceeded"]
)
async def test_serialized_complete_cannot_replace_original_native_proof(bounded_wave, monkeypatch, fault):
    admission = bounded_wave[2]
    original = fhir._record_profile_evidence_worker_outcome

    def record(*args, **kwargs):
        original(*args, **kwargs)
        owner = admission.wal_tracker.owned_evidence_worker_outcomes[args[1]]
        _corrupt_evidence_owner(owner, fault)

    monkeypatch.setattr(fhir, "_record_profile_evidence_worker_outcome", record)
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await run_wave(bounded_wave)
    tracker = admission.wal_tracker
    assert not tracker.owned_evidence_worker_outcomes
    assert tracker.owned_evidence_wave_native_outcomes[0]["status"] == "incomplete"
    assert tracker.owned_evidence_wave_outcomes[0]["status"] == "accounting_incomplete"
    assert tracker.owned_evidence_wave_outcomes[0]["workers"][0]["measurement_status"] != "complete"


@pytest.mark.asyncio
async def test_duplicate_coordinates_are_not_complete(bounded_wave):
    _, _, admission, build, batches, projection, _ = bounded_wave
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await fhir._run_profile_evidence_window(
            build, [(5, batches[0]), (5, batches[1])], "COPY unused", {}, {5: projection}
        )
    native = admission.wal_tracker.owned_evidence_wave_native_outcomes[0]
    assert native["status"] == "incomplete" and [worker["coordinate"] for worker in native["workers"]] == [5, 5]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["rollback", "cancel"])
async def test_diagnostic_failure_cannot_replace_original_worker_failure(bounded_wave, monkeypatch, mode):
    state, _, admission, _, _, _, _ = bounded_wave
    state.mode = mode
    original = fhir._profile_owned_transaction_capture

    def capture(outcome, failure):
        if failure is not None:
            raise RuntimeError("synthetic diagnostic failure")
        return original(outcome, failure)

    monkeypatch.setattr(fhir, "_profile_owned_transaction_capture", capture)
    with pytest.raises((custody.OwnedWalTransactionError, asyncio.CancelledError)) as raised:
        await run_wave(bounded_wave, 2)
    assert "synthetic diagnostic failure" not in str(raised.value)
    native = admission.wal_tracker.owned_evidence_wave_native_outcomes[0]
    assert native["status"] == "incomplete" and all(worker["task"].done() for worker in native["workers"])
    assert all(worker["owner"]["outcome"] is not None for worker in native["workers"])
    assert all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_consumer_failure_preserves_exact_gather_exception(bounded_wave, monkeypatch):
    failure = asyncio.CancelledError("synthetic cancellation")
    original_gather = fhir._gather_provider_directory_profile_tasks

    async def drained_failure(tasks):
        await original_gather(tasks)
        raise failure

    gather = AsyncMock(side_effect=drained_failure)
    monkeypatch.setattr(fhir, "_gather_provider_directory_profile_tasks", gather)
    monkeypatch.setattr(
        fhir, "_consume_profile_evidence_worker_outcomes", lambda *args: (_ for _ in ()).throw(ValueError())
    )
    monkeypatch.setattr(fhir, "_execute_owned_profile_evidence_batch", AsyncMock(return_value=1))
    with pytest.raises(asyncio.CancelledError) as raised:
        await run_wave(bounded_wave)
    assert raised.value is failure
    gather.assert_awaited_once()


@pytest.mark.asyncio
async def test_stray_worker_is_held_without_creating_another_wave(bounded_wave, monkeypatch):
    admission = bounded_wave[2]
    stray = asyncio.create_task(asyncio.sleep(0))
    await stray
    held_by_field = {"task": stray, "outcome": None}
    admission.wal_tracker.owned_evidence_worker_outcomes[stray] = held_by_field
    execute = AsyncMock()
    monkeypatch.setattr(fhir, "_execute_owned_profile_evidence_batch", execute)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="owned_capture_unconsumed"):
            await run_wave(bounded_wave)
    assert admission.wal_tracker.owned_evidence_worker_outcomes == {stray: held_by_field}
    assert not admission.wal_tracker.owned_evidence_wave_native_outcomes
    execute.assert_not_called()


def _corrupt_evidence_owner(owner_by_field, fault):
    outcome = owner_by_field["outcome"]
    if fault == "missing":
        owner_by_field["outcome"] = None
        return
    if fault == "witness":
        owner_by_field["statement_witness"] = None
        return
    if fault == "witness_exceeded":
        owner_by_field["statement_witness"] = replace(owner_by_field["statement_witness"], wal_record_bytes=129)
        return
    if fault == "identity":
        outcome.measurement = replace(
            outcome.measurement,
            final=replace(outcome.measurement.final, identity=replace(outcome.measurement.final.identity, pid=1)),
        )
        return
    fields_by_fault = {"commit": "commit_state", "cleanup": "cleanup_complete", "measurement": "measurement"}
    replacement_by_fault = {"commit": "unknown", "cleanup": False, "measurement": None}
    setattr(outcome, fields_by_fault[fault], replacement_by_fault[fault])


@pytest.mark.asyncio
async def test_original_global_overrun_still_holds_pending_exposure(bounded_wave, monkeypatch):
    assert await run_wave(bounded_wave) == [1]
    retained = bounded_wave[2].wal_tracker.owned_evidence_wave_native_outcomes[0]
    owner = object()
    tracker = fhir._ProviderDirectoryProfileWalTracker(pending_control_wal_bytes={owner: 73728})
    geometry = SimpleNamespace(
        bounded_admission=True,
        reservation_bytes_by_storage_class={"wal": 2702351},
        relation_byte_caps=(),
        metadata_wal_upper_bound_bytes=0,
    )
    admission = replace(bounded_wave[2], wal_tracker=tracker, geometry=geometry)
    observed = AsyncMock(return_value=2702352)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", AsyncMock())
    monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", observed)
    with pytest.raises(RuntimeError, match="total_wal_projected"):
        await fhir.profile_capacity_projection._settle_mutation_window(fhir, admission, owner, None, 627696, 0)
    assert tracker.pending_control_wal_bytes == {owner: 73728}
    assert not tracker.accounted_relation_wal_bytes and not tracker.accounted_control_operation_counts
    assert retained["status"] == "complete" and not retained["accounting_authority"]
    assert observed.await_count == 2
    observed.assert_awaited_with(admission)


@pytest.mark.asyncio
async def test_missing_witness_preserves_authentic_committed_outcome(bounded_wave, monkeypatch):
    admission = bounded_wave[2]
    record_outcome = fhir._record_profile_evidence_worker_outcome

    def omit_witness(*args, **kwargs):
        record_outcome(*args, **kwargs)
        admission.wal_tracker.owned_evidence_worker_outcomes[args[1]]["statement_witness"] = None

    monkeypatch.setattr(fhir, "_record_profile_evidence_worker_outcome", omit_witness)
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await run_wave(bounded_wave)
    native = admission.wal_tracker.owned_evidence_wave_native_outcomes[0]
    outcome = native["workers"][0]["owner"]["outcome"]
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[0]["workers"][0]
    assert outcome.status == "committed_measured" and outcome.commit_state == "confirmed"
    assert outcome.cleanup_complete and outcome.measurement is not None
    assert capture["status"] == "committed_accounting_incomplete" and capture["committed"] is True
    assert capture["measurement_status"] == "incomplete" and capture["measurement"] is None
    assert native["status"] == "incomplete" and not native["accounting_authority"]
