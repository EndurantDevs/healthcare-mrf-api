# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original control owners, commit truth and incomplete borrowed exposure."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_provider_directory_owned_evidence_wave import custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

pytestmark = pytest.mark.asyncio


@pytest.fixture
def control_wave(owned_wave, monkeypatch):
    state, database, admission, *rest = owned_wave
    admission = replace(
        admission,
        geometry=replace(
            admission.geometry,
            physical_projection_contract_id=fhir.profile_capacity.BOUNDED_ADMISSION_CONTRACT_ID,
        ),
    )
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    return state, database, admission, *rest


async def test_checkpoint_status_uses_one_original_native_terminal_owner(control_wave):
    state, database, admission, *_ = control_wave
    statement = "UPDATE synthetic_checkpoint SET cursor=:cursor;"
    assert await fhir._provider_directory_profile_capacity_status(statement, cursor=2) == 1
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert group["identity"] == (statement, {"cursor": 2})
    assert group["outcome"] is outcome and group["session"] is outcome.session
    assert group["connection"] is state.connections[0] and group["driver"] is outcome.retained_driver
    assert group["consumed"] and group["status"] == "complete_mixed_unclassified"
    assert not group["accounting_authority"] and not group["reservation_refund"]
    assert outcome.status == "committed_measured" and outcome.cleanup_complete
    assert database._transaction_binding() is None
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


@pytest.mark.parametrize("mode", ["rollback", "cancel", "postcommit_sample", "unknown_commit", "restore_failure"])
async def test_native_control_failure_keeps_truth_and_incomplete_exposure(control_wave, mode):
    state, _, admission, *_ = control_wave
    state.mode = mode
    expected = asyncio.CancelledError if mode == "cancel" else Exception
    with pytest.raises(expected):
        await fhir._provider_directory_profile_capacity_status("UPDATE synthetic_checkpoint SET cursor=2;")
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert group["outcome"] is outcome and not group["consumed"] and group["status"] == "incomplete"
    assert group["failure"] is not None and outcome.cleanup_complete
    assert not group["accounting_authority"] and not group["reservation_refund"]
    if mode in ["postcommit_sample", "restore_failure"]:
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert outcome.commit_state == "attempted" and not outcome.is_committed
    else:
        assert outcome.commit_state == "rolled_back"
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


async def test_body_failure_is_original_exception_object(control_wave):
    _, _, admission, *_ = control_wave
    primary = OSError("synthetic control body failure")
    with pytest.raises(OSError) as caught:
        async with fhir._provider_directory_profile_capacity_transaction(control_identity=("synthetic",)):
            raise primary
    assert caught.value is primary
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["failure"] is primary and not group["consumed"]


async def test_borrowed_group_waits_for_actual_outer_native_outcome(control_wave):
    state, database, admission, *_ = control_wave
    async with custody.registry_owned_wal_transaction(database) as outcome:
        assert custody.current_owned_wal_transaction(database) is outcome
        async with fhir._provider_directory_profile_capacity_transaction(
            reuse_borrowed=True,
            control_identity=("synthetic_import_progress",),
        ):
            assert await database.status("UPDATE synthetic_import_run SET progress_by_field=1;") == 1
        group = admission.wal_tracker.owned_control_transaction_groups[0]
        assert group["original_outcome"] is outcome and not group["consumed"]
        assert not outcome.is_committed and outcome.measurement is None
    assert custody.current_owned_wal_transaction(database) is None
    fhir._profile_owned_transaction_capture(outcome, None)
    assert group["consumed"] and group["outcome"] is outcome
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


async def test_missing_containing_owner_never_fabricates_inner_completion(control_wave):
    _, database, admission, *_ = control_wave
    async with custody.registry_owned_wal_transaction(database) as outcome:
        token = custody._ACTIVE_OWNED_TRANSACTION.set(None)
        try:
            async with fhir._provider_directory_profile_capacity_transaction(
                reuse_borrowed=True,
                control_identity=("synthetic_import_progress",),
            ):
                assert await database.status("UPDATE synthetic_import_run SET progress=1;") == 1
        finally:
            custody._ACTIVE_OWNED_TRANSACTION.reset(token)
    fhir._profile_owned_transaction_capture(outcome, None)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["original_outcome"] is None and group["outcome"] is None
    assert not group["consumed"] and group["status"] == "incomplete"
    assert not group["accounting_authority"] and not group["reservation_refund"]


@pytest.mark.parametrize(
    "field",
    ["task", "window", "admission", "identity", "original_outcome", "outcome", "driver", "pid", "body_complete"],
)
async def test_misbound_control_custody_preserves_authentic_commit(control_wave, monkeypatch, field):
    _, _, admission, *_ = control_wave
    consume = fhir.profile_control_custody.consume_owned_groups

    def corrupt(module, outcome, failure, *, groups=None):
        groups[0][field] = object()
        consume(module, outcome, failure, groups=groups)

    monkeypatch.setattr(fhir.profile_control_custody, "consume_owned_groups", corrupt)
    with pytest.raises(RuntimeError, match="control_custody_incomplete"):
        await fhir._provider_directory_profile_capacity_status("UPDATE synthetic_checkpoint SET cursor=2;")
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["outcome"] if field == "original_outcome" else group["original_outcome"]
    assert outcome.is_committed and outcome.status == "committed_measured" and outcome.cleanup_complete
    assert not group["consumed"] and group["status"] == "incomplete"


async def test_admitted_import_progress_records_original_group_identity(control_wave, monkeypatch):
    _, database, admission, *_ = control_wave
    monkeypatch.setattr(fhir, "_schema", lambda: "synthetic")
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(
            return_value=type("Layout", (), {"exact_fingerprint": admission.geometry.import_run_storage_fingerprint})(),
        ),
    )

    async def is_marked(run_id, **params):
        assert run_id == "synthetic_run" and params["status"] == "running"
        assert await database.status("UPDATE synthetic_import_run SET progress_by_field=1;") == 1
        return True

    monkeypatch.setattr(fhir, "mark_control_run", is_marked)
    progress_by_field = {"done": 1}
    assert await fhir._is_admitted_profile_progress_written(
        "synthetic_run",
        admission,
        phase="synthetic_phase",
        message="synthetic progress_by_field",
        progress_by_name=progress_by_field,
        metrics=None,
    )
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["identity"][0:3] == ("import_run_progress", "synthetic_run", "synthetic_phase")
    assert group["identity"][3] is progress_by_field and group["consumed"]


async def test_completed_control_storage_retains_only_current_parent(control_wave):
    _, _, admission, *_ = control_wave
    for _ in range(5):
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), None))
        try:
            await fhir._provider_directory_profile_capacity_status("UPDATE synthetic_checkpoint SET cursor=2;")
            assert len(admission.wal_tracker.owned_control_transaction_groups) == 1
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


async def test_capacity_admission_keeps_original_consumption_owner_before_tracker_exists(control_wave, monkeypatch):
    _, database, admission, *_ = control_wave
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: object())
    monkeypatch.setattr(fhir, "_schema", lambda: "synthetic")
    for name in [
        "_lock_profile_capacity_preflight_state",
        "_lock_provider_directory_profile_capacity_control_run",
        "_assert_profile_capacity_build_unconsumed",
        "assert_profile_selection_current_in_transaction",
        "_consume_profile_capacity_preflight_receipt",
        "_assert_admission_run_toast",
    ]:
        monkeypatch.setattr(fhir, name, AsyncMock())
    monkeypatch.setattr(fhir, "_locked_profile_admission_serving_state", AsyncMock(return_value=object()))
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_capacity_serving_state", Mock())
    original_database_identity = object()
    monkeypatch.setattr(
        fhir, "_profile_admission_runtime_state", AsyncMock(return_value=(original_database_identity, {}))
    )

    real_status = database.status

    async def bounded_status(statement, **params):
        owner = custody.current_owned_wal_transaction(database)
        owner.connection.events.append("limits")
        return await real_status(statement, **params)

    monkeypatch.setattr(database, "status", bounded_status)

    async def consume_values(lease, binding):
        assert await database.status("INSERT INTO synthetic_capacity_consumption VALUES (1);") == 1

    monkeypatch.setattr(fhir, "_consume_admission_values", consume_values)
    identity = SimpleNamespace(initial_targets=None, serving_state=object())
    binding = SimpleNamespace(build_id="synthetic_build")
    lease = object()
    workload = SimpleNamespace(database_identity=object(), control_wal_plan_input=object())
    control_groups = []
    observed = await fhir._consume_admission_transaction(
        "synthetic_run",
        SimpleNamespace(attestation=object()),
        identity,
        lease,
        binding,
        workload,
        admission.geometry,
        control_groups=control_groups,
    )
    assert observed is original_database_identity and len(control_groups) == 1
    group = control_groups[0]
    assert group["identity"] == ("capacity_admission", binding, lease, identity)
    assert group["admission"] is None and group["consumed"]
    assert group["outcome"] is group["original_outcome"] and group["outcome"].is_committed
