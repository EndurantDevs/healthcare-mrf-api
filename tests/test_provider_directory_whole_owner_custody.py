# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual original owners retain setup/restoration observations and unknown tails."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from process import provider_directory_backend_wal_diagnostic as diagnostic
from process import provider_directory_owned_wal_transaction as owner
from tests.test_provider_directory_backend_wal_diagnostic import BoundConnection, _preflight, _sample
from tests.test_provider_directory_owned_evidence_wave import Driver
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave
from tests.test_provider_directory_retained_maintenance_coupled import close_healthy, coupled_fixture

pytestmark = pytest.mark.asyncio


@pytest.fixture
def boundary_samples(monkeypatch):
    original = Driver.fetchrow
    samples = []
    gate = asyncio.Event()
    started = asyncio.Event()
    gate.set()

    async def fetchrow(driver, statement, *parameters):
        if statement != diagnostic._OWNER_BOUNDARY_SAMPLE_SQL:
            return await original(driver, statement, *parameters)
        assert parameters == (driver.pid,) and not driver.in_transaction
        events = driver.connection.events
        assert driver.settings == driver.original
        is_final = "commit" in events
        if is_final:
            started.set()
            await gate.wait()
        else:
            assert "begin" not in events
        events.append("owner_final" if is_final else "owner_baseline")
        snapshot = _sample(
            pid=driver.pid,
            wal_records=40 if is_final else 1,
            wal_fpi=4 if is_final else 1,
            wal_buffers_full=3 if is_final else 0,
            wal_bytes=Decimal("640") if is_final else Decimal("16"),
            insert_lsn="0/1000" if is_final else "0/10",
        )
        samples.append((driver, snapshot))
        return snapshot

    monkeypatch.setattr(Driver, "fetchrow", fetchrow)
    return samples, gate, started


async def test_actual_standalone_owner_samples_before_settings_and_after_original_restoration(
    owned_wave, boundary_samples
):
    state, database, *_ = owned_wave
    async with owner.registry_owned_wal_transaction(database) as outcome:
        driver = outcome.retained_driver
        assert driver.settings["max_parallel_workers"] == "0"
        assert outcome.owner_measurement is None
    connection = state.connections[0]
    assert connection.events.index("owner_baseline") < connection.events.index("begin")
    assert (
        connection.events.index("commit")
        < connection.events.index("owner_final")
        < connection.events.index("pool_return")
    )
    assert driver.settings == driver.original and driver.original["max_parallel_workers"] == "8"
    assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
    assert outcome.owner_measurement.baseline.identity == outcome.owner_measurement.final.identity
    assert outcome.owner_measurement.final.identity.pid == driver.pid
    assert outcome.owner_measurement.wal_bytes_delta == Decimal("624")
    assert outcome.owner_measurement.wal_bytes_delta > outcome.measurement.wal_bytes_delta
    assert outcome.owner_measurement_complete is False


async def test_actual_retained_owner_keeps_original_release_and_restoration(boundary_samples):
    fhir, connection, events, admission, captures = await coupled_fixture()
    async with fhir.profile_owned_wal.registry_retained_wal_transaction(connection) as outcome:
        assert connection.in_transaction()
    assert not connection.closed and connection.driver.settings == connection.driver.original
    assert outcome.is_committed and outcome.cleanup_complete and outcome.owner_measurement is not None
    assert outcome.owner_measurement_complete is False
    assert connection.events.count("begin") == connection.events.count("commit") == 1
    assert connection.events.index("commit") < connection.events.index("owner_final")
    await close_healthy(connection)


@pytest.mark.parametrize("mode", ["rollback", "cancel", "unknown_commit", "restore_failure", "postcommit_sample"])
async def test_original_failures_cancellation_and_commit_truth_keep_unknown_owner_exposure(
    owned_wave, boundary_samples, mode
):
    state, database, *_ = owned_wave
    state.mode = mode
    primary = ValueError("synthetic original body failure")
    expected = owner.OwnedWalTransactionCancelled if mode == "cancel" else owner.OwnedWalTransactionError
    with pytest.raises(expected) as caught:
        async with owner.registry_owned_wal_transaction(database) as outcome:
            if mode == "rollback":
                raise primary
            if mode == "cancel":
                asyncio.current_task().cancel()
                await asyncio.sleep(0)
            if mode == "postcommit_sample":
                outcome.retained_driver.fail_command = "sample"
                outcome.retained_driver.error = OSError("synthetic body sample failure")
    assert caught.value.outcome is outcome and outcome.cleanup_complete
    assert outcome.owner_measurement is None and outcome.owner_measurement_complete is False
    if mode == "rollback":
        assert caught.value.__cause__ is primary and outcome.commit_state == "rolled_back"
    elif mode == "unknown_commit":
        assert outcome.commit_state == "attempted" and not outcome.is_committed
    elif mode in {"restore_failure", "postcommit_sample"}:
        assert outcome.is_committed
    assert state.connections[0].closed and state.connections[0].driver.closed


async def test_cancellation_during_original_drained_cleanup_retains_committed_truth(owned_wave, boundary_samples):
    _state, database, *_ = owned_wave
    _samples, gate, started = boundary_samples
    gate.clear()
    outcomes = []

    async def run():
        async with owner.registry_owned_wal_transaction(database) as outcome:
            outcomes.append(outcome)

    task = asyncio.create_task(run())
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    gate.set()
    with pytest.raises(owner.OwnedWalTransactionCancelled) as caught:
        await task
    outcome = outcomes[0]
    assert caught.value.outcome is outcome and outcome.is_committed and outcome.cleanup_complete
    assert outcome.measurement is None and outcome.owner_measurement is not None
    assert outcome.owner_measurement_complete is False


async def test_boundary_native_scalar_route_accepts_original_settings_without_weakening_body():
    connection = BoundConnection(preflight=_preflight(parallel_workers=8, parallel_gather=2, debug_parallel="on"))
    snapshot = await diagnostic.sample_backend_wal_owner_boundary(connection, connection.pid)
    assert snapshot.identity.pid == connection.pid and connection.commands == ["clear", "force", "owner_sample"]
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)
    assert connection.commands[-1] == "preflight"
    assert "pg_catalog.pg_control_system()" in diagnostic._OWNER_BOUNDARY_SAMPLE_SQL
    assert "pg_catalog.pg_stat_get_activity($1)" in diagnostic._OWNER_BOUNDARY_SAMPLE_SQL
    assert "pg_stat_activity" not in diagnostic._OWNER_BOUNDARY_SAMPLE_SQL


@pytest.mark.parametrize("state", ["transaction", "closed", "wrong_pid"])
async def test_boundary_native_custody_refuses_before_commands(state):
    connection = BoundConnection()
    connection.in_transaction = state == "transaction"
    connection.closed = state == "closed"
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.sample_backend_wal_owner_boundary(connection, 1 if state == "wrong_pid" else connection.pid)
    assert connection.commands == []


@pytest.mark.parametrize("at_final", [False, True])
async def test_unavailable_boundary_observation_preserves_original_success(
    owned_wave, boundary_samples, monkeypatch, at_final
):
    _state, database, *_ = owned_wave
    original = owner.sample_backend_wal_owner_boundary
    calls = []

    async def unavailable(driver, pid):
        calls.append(driver)
        if (len(calls) == 2) is at_final:
            raise diagnostic.BackendWalDiagnosticUnavailable()
        return await original(driver, pid)

    monkeypatch.setattr(owner, "sample_backend_wal_owner_boundary", unavailable)
    async with owner.registry_owned_wal_transaction(database) as outcome:
        assert outcome.session.in_transaction()
    assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
    assert outcome.owner_measurement is None and outcome.owner_measurement_complete is False
    assert len(calls) == 2 and calls[0] is calls[1] is outcome.retained_driver


async def test_original_body_failure_keeps_priority_over_original_retirement_failure(owned_wave, boundary_samples):
    state, database, *_ = owned_wave
    primary = ValueError("synthetic original body failure")
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with owner.registry_owned_wal_transaction(database) as outcome:
            original = outcome.retained_driver.terminate
            attempts = []

            def terminate():
                attempts.append(True)
                if len(attempts) == 1:
                    raise OSError("synthetic original retirement failure")
                original()

            outcome.retained_driver.terminate = terminate
            raise primary
    assert caught.value.__cause__ is primary and outcome.cleanup_complete
    assert outcome.owner_measurement is None and outcome.owner_measurement_complete is False
    assert state.checkedout == 0


@pytest.mark.parametrize(
    "change", [{"pid": 99}, {"wal_records": 0}, {"wal_records": 3}, {"wal_bytes": Decimal("0")}, {"database_oid": 124}]
)
async def test_final_boundary_counter_or_identity_mismatch_stays_held(owned_wave, boundary_samples, change):
    _state, database, *_ = owned_wave
    async with owner.registry_owned_wal_transaction(database) as outcome:
        driver = outcome.retained_driver
        original = driver.fetchrow

        async def fetchrow(statement, *parameters):
            result = await original(statement, *parameters)
            if statement == diagnostic._OWNER_BOUNDARY_SAMPLE_SQL:
                return {**result, **change}
            return result

        driver.fetchrow = fetchrow
    assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
    assert outcome.owner_measurement is None and outcome.owner_measurement_complete is False


async def test_final_wrapper_driver_misbinding_holds_original_owner_observation(owned_wave, boundary_samples):
    _state, database, *_ = owned_wave
    async with owner.registry_owned_wal_transaction(database) as outcome:
        connection = outcome.connection
        original = connection.get_raw_connection

        async def get_raw_connection():
            raw = await original()
            if "commit" in connection.events and outcome.retained_driver.settings == outcome.retained_driver.original:
                return SimpleNamespace(driver_connection=object())
            return raw

        connection.get_raw_connection = get_raw_connection
    assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
    assert outcome.owner_measurement is None and outcome.owner_measurement_complete is False
    assert "owner_final" not in connection.events and connection.closed
