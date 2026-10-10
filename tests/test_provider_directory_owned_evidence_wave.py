# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual evidence owner/capacity/gather paths with a pool-one fake driver."""

import asyncio
import copy
import importlib
import json
import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import Database
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_backend_wal_diagnostic import BoundConnection

fhir = importlib.import_module("process.provider_directory_fhir")
custody = fhir.profile_owned_wal


class Driver(BoundConnection):
    def __init__(self, connection, pid):
        super().__init__()
        self.connection, self.pid = connection, pid
        self.preflight["pid"] = pid
        for sample in self.samples:
            sample["pid"] = pid
        self.settings = dict(zip(custody._SETTINGS, ("8", "2", "2", "on", '"$user", public')))
        self.original = dict(self.settings)

    def terminate(self):
        self.connection.events.append("native_terminate")
        self.closed = True
        self.in_transaction = False

    async def fetchrow(self, statement, *parameters):
        if statement == custody._READ_SETTINGS:
            assert not self.in_transaction
            return dict(self.settings)
        return await super().fetchrow(statement, *parameters)

    async def execute(self, statement, *parameters):
        if statement == custody._SET_SETTING:
            assert not self.in_transaction
            name, value = parameters
            if (
                self.connection.state.mode == "restore_failure"
                and "commit" in self.connection.events
                and value == self.original[name]
            ):
                raise OSError("synthetic settings restore failure")
            self.settings[name] = value
            return "SELECT 1"
        return await super().execute(statement)

    async def _command(self, name):
        self.connection.events.append(name)
        return await super()._command(name)


class Connection:
    def __init__(self, state, ordinal):
        self.state, self.events = state, []
        self.driver = Driver(self, 4321 + ordinal)
        self.url = SimpleNamespace(database="synthetic")
        self.sync_connection = None
        self.closed = self.invalidated = False

    async def start(self):
        await self.state.pool.acquire()
        self.state.checkedout += 1
        self.state.max_checkedout = max(self.state.max_checkedout, self.state.checkedout)
        self.sync_connection = object()
        self.events.append("checkout")
        return self

    async def get_raw_connection(self):
        return SimpleNamespace(driver_connection=self.driver)

    async def invalidate(self):
        self.invalidated = self.driver.closed = True
        self.driver.in_transaction = False
        self.events.append("invalidate")

    async def close(self):
        assert not self.driver.in_transaction
        assert self.invalidated or self.driver.settings == self.driver.original
        self.closed = True
        self.events.append("pool_return")
        self.state.checkedout -= 1
        self.state.pool.release()


class Session(AsyncSession):
    # This fixture retains a synthetic connection without SQLAlchemy bind translation.
    bind = None

    def __init__(self, *, bind, expire_on_commit, autoflush):
        assert not expire_on_commit and not autoflush
        self.bind, self.active = bind, False
        self.has_nested_transaction = False

    def in_transaction(self):
        return self.active

    def in_nested_transaction(self):
        return self.has_nested_transaction

    def begin_nested(self):
        raise AssertionError("evidence owner must retain its top-level transaction")

    async def begin(self):
        self.active = self.bind.driver.in_transaction = True
        self.bind.events.append("begin")

    async def commit(self):
        self.bind.events.append("commit")
        self.active = self.bind.driver.in_transaction = False
        if self.bind.state.mode == "unknown_commit":
            raise OSError("synthetic lost commit response")

    async def rollback(self):
        self.bind.events.append("rollback")
        self.active = self.bind.driver.in_transaction = False

    async def close(self):
        self.bind.events.append("session_close")

    async def execute(self, statement, parameters):
        assert self.active and "limits" in self.bind.events
        self.bind.events.append("write")
        mode = self.bind.state.mode
        if mode == "rollback":
            raise ValueError("synthetic statement failure")
        if mode == "cancel":
            asyncio.current_task().cancel()
            await asyncio.sleep(0)
        if mode == "postcommit_sample":
            self.bind.driver.fail_command, self.bind.driver.error = "sample", OSError("synthetic observation failure")
        rows = 3 if mode == "overrun" else 1
        plan_documents = [
            {
                "Plan": {
                    "Node Type": "ModifyTable",
                    "Operation": "Insert",
                    "Actual Loops": 1,
                    "Actual Rows": 0.0,
                    "Tuples Inserted": rows,
                    "Conflicting Tuples": 2,
                    "Conflict Resolution": "NOTHING",
                    "WAL Records": 2,
                    "WAL FPI": 1,
                    "WAL Bytes": 96,
                }
            }
        ]
        return SimpleNamespace(rowcount=rows, scalar=lambda: plan_documents)


def ledger(tracker):
    return copy.deepcopy(
        {
            name: getattr(tracker, name)
            for name in (
                "accounted_control_operation_counts",
                "accounted_relation_wal_bytes",
                "accounted_metadata_wal_bytes",
                "pending_relation_wal_bytes",
                "pending_control_wal_bytes",
                "pending_metadata_wal_bytes",
                "pending_growth_bytes",
            )
        }
    )


@pytest.fixture
def owned_wave(monkeypatch):
    state = SimpleNamespace(pool=asyncio.Semaphore(1), checkedout=0, max_checkedout=0, connections=[], mode="success")

    def connect():
        connection = Connection(state, len(state.connections))
        state.connections.append(connection)
        return connection

    database = Database()
    database._database_override = "synthetic"
    database.engine = SimpleNamespace(connect=connect, dialect=SimpleNamespace(name="postgresql", driver="asyncpg"))
    database.session_factory = Mock(side_effect=AssertionError("no second checkout under pool one"))
    database.connect = AsyncMock(side_effect=AssertionError("no reconnect"))
    database.disconnect = AsyncMock(side_effect=AssertionError("no disconnect"))
    admission = _wal_tracker_admission()
    admission.wal_tracker.pending_relation_wal_bytes["evidence_stage"] = 2048
    admission.wal_tracker.pending_metadata_wal_bytes = 1024
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(custody, "AsyncSession", Session)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_stage_storage_identity", AsyncMock())
    monkeypatch.setattr(
        fhir.profile_artifact,
        "profile_evidence_insert_sql",
        Mock(return_value="INSERT INTO synthetic_evidence SELECT 1 ON CONFLICT (evidence_key) DO NOTHING;"),
    )

    async def limits(_admission):
        binding = database._transaction_binding(borrowed_only=True)
        assert binding.session.in_transaction() and not binding.session.in_nested_transaction()
        binding.session.bind.events.append("limits")

    monkeypatch.setattr(fhir, "_apply_provider_directory_profile_capacity_settings", limits)
    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    build = SimpleNamespace(schema="synthetic", evidence_stage="synthetic_evidence", profile_as_of="2026-01-01")
    batches = [
        fhir._ProviderDirectoryProfileEvidenceBatch(
            kind="fact", source_id="synthetic_source", dataset_id="synthetic_dataset", fact_type="name"
        )
        for _ in range(2)
    ]
    projection = fhir._ProviderDirectoryProfileEvidenceProjection(projected_rows=2, projected_logical_bytes=256)
    original_ledger = ledger(admission.wal_tracker)
    try:
        yield state, database, admission, build, batches, projection, original_ledger
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
        assert ledger(admission.wal_tracker) == original_ledger
        assert database._transaction_binding() is None and state.checkedout == 0
        database.connect.assert_not_awaited()
        database.disconnect.assert_not_awaited()
        database.session_factory.assert_not_called()


async def run_wave(fixture, count=1):
    _state, _database, _admission, build, batches, projection, _ledger = fixture
    return await fhir._run_profile_evidence_window(
        build, list(enumerate(batches[:count])), "COPY unused", {}, {index: projection for index in range(count)}
    )


@pytest.mark.asyncio
async def test_real_evidence_workers_commit_capture_and_consume_with_pool_one(owned_wave):
    state, _database, admission, _build, _batches, _projection, _ledger = owned_wave
    assert await run_wave(owned_wave, 2) == [1, 1]
    assert len(state.connections) == 2 and state.max_checkedout == 1
    assert admission.wal_tracker.owned_evidence_worker_outcomes == {}
    wave = admission.wal_tracker.owned_evidence_wave_outcomes[-1]
    assert wave["expected_workers"] == wave["drained_workers"] == 2
    assert wave["status"] == "committed_measured_mixed_unclassified"
    assert not wave["accounting_authority"] and not wave["reservation_refund"]
    for connection, capture in zip(state.connections, wave["workers"], strict=True):
        events = connection.events
        assert events.index("limits") < events.index("write") < events.index("commit")
        assert events.index("commit") < len(events) - 1 - events[::-1].index("sample")
        assert connection.closed and connection.driver.settings == connection.driver.original
        assert capture["measurement"]["wal_record_bytes"] == "128"
        assert capture["measurement"]["global_physical_lsn_span"] == 256
        assert capture["wal_classification"] == "mixed_unclassified"


@pytest.mark.asyncio
async def test_completed_wave_capture_storage_stays_bounded(owned_wave):
    """Large imports retain only the current wave after observations consume it."""
    _state, _database, admission, _build, _batches, _projection, _ledger = owned_wave
    assert await run_wave(owned_wave) == [1]
    earlier_wave = admission.wal_tracker.owned_evidence_wave_outcomes[0]
    assert await run_wave(owned_wave, 2) == [1, 1]
    assert len(admission.wal_tracker.owned_evidence_wave_outcomes) == 1
    current_wave = admission.wal_tracker.owned_evidence_wave_outcomes[0]
    assert current_wave is not earlier_wave
    assert current_wave["expected_workers"] == 2 and earlier_wave["expected_workers"] == 1
    assert admission.wal_tracker.owned_evidence_worker_outcomes == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["rollback", "overrun", "cancel", "postcommit_sample", "unknown_commit", "restore_failure"]
)
async def test_worker_failures_preserve_commit_truth_and_ledger(owned_wave, mode):
    state, _database, admission, _build, _batches, _projection, _ledger = owned_wave
    state.mode = mode
    error_type = asyncio.CancelledError if mode == "cancel" else custody.OwnedWalTransactionError
    with pytest.raises(error_type):
        await run_wave(owned_wave)
    wave = admission.wal_tracker.owned_evidence_wave_outcomes[-1]
    assert wave["expected_workers"] == wave["drained_workers"] == 1
    assert wave["status"] == "accounting_incomplete" and not wave["reservation_refund"]
    capture = wave["workers"][0]
    assert capture["measurement"] is None and capture["cleanup_complete"]
    if mode in {"postcommit_sample", "restore_failure"}:
        assert capture["committed"] and capture["status"] == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert capture["commit_state"] == "attempted" and capture["status"] == "commit_uncertain_accounting_incomplete"
    else:
        assert capture["commit_state"] == "rolled_back"
        assert "commit" not in state.connections[0].events
    assert all(connection.closed for connection in state.connections)
    if mode == "restore_failure":
        assert state.connections[0].invalidated


@pytest.mark.asyncio
async def test_missing_worker_capture_fails_closed_without_fabricating_observation(owned_wave, monkeypatch):
    _state, _database, admission, _build, _batches, _projection, _ledger = owned_wave
    monkeypatch.setattr(fhir, "_execute_owned_profile_evidence_batch", AsyncMock(return_value=1))
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await run_wave(owned_wave)
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[-1]["workers"][0]
    assert capture["commit_state"] == "unobserved" and capture["committed"] is None
    assert capture["measurement"] is None and not capture["reservation_refund"]


@pytest.mark.asyncio
async def test_failure_drains_all_pool_one_siblings_before_consuming_capture(owned_wave):
    state, _database, admission, _build, _batches, _projection, _ledger = owned_wave
    state.mode = "rollback"
    with pytest.raises(custody.OwnedWalTransactionError):
        await run_wave(owned_wave, 2)
    wave = admission.wal_tracker.owned_evidence_wave_outcomes[-1]
    assert wave["expected_workers"] == wave["drained_workers"] == 2
    assert len(wave["workers"]) == 2 and admission.wal_tracker.owned_evidence_worker_outcomes == {}
    assert all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_duplicate_worker_cannot_prove_complete_coverage(owned_wave):
    _state, _database, admission, build, batches, _projection, _ledger = owned_wave
    worker = asyncio.create_task(asyncio.sleep(0))
    await worker
    fhir._record_profile_evidence_worker_outcome(
        admission, worker, None, None, build=build, coordinate=(0, batches[0]), window=None
    )
    wave_outcome = fhir._consume_profile_evidence_worker_outcomes(
        admission, build, [worker, worker], list(enumerate(batches))
    )
    assert wave_outcome["status"] == "accounting_incomplete"
    wave = admission.wal_tracker.owned_evidence_wave_outcomes[-1]
    assert wave["expected_workers"] == 2 and wave["drained_workers"] == 1
    assert not wave["reservation_refund"]


@pytest.mark.asyncio
async def test_existing_observation_consumes_raw_wave_capture(owned_wave, monkeypatch, caplog):
    monkeypatch.setenv(fhir.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV, "1")
    monkeypatch.setattr(
        fhir,
        "_profile_capacity_observation_sample",
        AsyncMock(
            return_value={
                "wal_lsn": "0/10",
                "wal_bytes": 0,
                "temp_bytes": 0,
                "relation_bytes": {"total": 0},
            }
        ),
    )
    caplog.set_level(logging.INFO, logger=fhir.__name__)
    await run_wave(owned_wave)
    observations = [
        json.loads(record.message)
        for record in caplog.records
        if "profile-clone-capacity-observation.v1" in record.message
    ]
    capture = observations[-1]["owned_worker_capture"]
    assert capture["status"] == "committed_measured_mixed_unclassified"
    assert capture["workers"][0]["measurement"]["units"] == "native_record_bytes"


@pytest.mark.asyncio
async def test_unadmitted_worker_preserves_legacy_executor_without_custody(monkeypatch):
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    executor = AsyncMock(return_value=7)
    owner = Mock(side_effect=AssertionError("legacy path cannot acquire custody"))
    monkeypatch.setattr(fhir, "_execute_profile_evidence_batch", executor)
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    assert await fhir._execute_owned_profile_evidence_batch("build", "batch", "copy", "refs", "projection") == 7
    executor.assert_awaited_once_with("build", "batch", "copy", "refs", "projection")
    owner.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["postcommit_sample", "unknown_commit"])
async def test_actual_outer_mutation_gate_retains_exposure_on_incomplete_worker(owned_wave, monkeypatch, mode):
    state, _database, admission, build, batches, projection, original_ledger = owned_wave
    state.mode = mode
    # Exercise the existing bounded gate without changing any synthetic cap.
    admission = replace(
        admission,
        geometry=replace(
            admission.geometry, physical_projection_contract_id=fhir.profile_capacity.BOUNDED_ADMISSION_CONTRACT_ID
        ),
    )
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", AsyncMock(return_value=1000))
    monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(return_value=0))
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_relation_bytes", AsyncMock(return_value=0))
    monkeypatch.setattr(fhir, "_preflight_profile_evidence_window_capacity", AsyncMock(return_value={0: projection}))
    settlement = AsyncMock(side_effect=AssertionError("uncertain worker cannot settle reservations"))
    monkeypatch.setattr(fhir.profile_capacity_projection, "_settle_mutation_window", settlement)
    with pytest.raises(custody.OwnedWalTransactionError):
        await fhir._execute_bounded_evidence_window(build, [(0, batches[0])], "COPY unused", {})
    assert admission.wal_tracker.unresolved_window
    assert ledger(admission.wal_tracker) == original_ledger
    settlement.assert_not_awaited()
    assert fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get() is None
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[-1]["workers"][0]
    assert capture["status"] == (
        "committed_accounting_incomplete" if mode == "postcommit_sample" else "commit_uncertain_accounting_incomplete"
    )
    assert not capture["reservation_refund"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "saved_path",
    [
        '"$user", public',
        "fixture_functions, fixture_operators, pg_temp, pg_catalog",
        "pg_temp, fixture_types, pg_catalog",
    ],
)
async def test_owner_catalog_path_precedes_diagnostic_and_restores_saved_namespace(owned_wave, monkeypatch, saved_path):
    """Harden before sampling, keep the borrowed connection, then restore its path."""
    state, database, *_ = owned_wave
    original_begin = custody.begin_backend_wal_diagnostic
    original_initialize = Driver.__init__
    observed_paths = []

    def initialize(driver, *arguments):
        original_initialize(driver, *arguments)
        driver.settings["search_path"] = saved_path
        driver.original = dict(driver.settings)

    monkeypatch.setattr(Driver, "__init__", initialize)

    async def begin(driver):
        observed_paths.append(driver.settings["search_path"])
        assert driver.settings["search_path"] == "pg_catalog, pg_temp"
        assert not driver.in_transaction
        return await original_begin(driver)

    monkeypatch.setattr(custody, "begin_backend_wal_diagnostic", begin)
    async with custody.registry_owned_wal_transaction(database) as outcome:
        binding = database._transaction_binding(borrowed_only=True)
        assert binding.session is outcome.session
        assert outcome.session.bind.driver.settings["search_path"] == "pg_catalog, pg_temp"
    connection = state.connections[0]
    assert observed_paths == ["pg_catalog, pg_temp"]
    assert outcome.is_committed and outcome.status == "committed_measured"
    assert connection.driver.settings["search_path"] == saved_path
    assert connection.closed and not connection.invalidated


@pytest.mark.asyncio
async def test_owner_path_readback_mismatch_retires_before_baseline_or_payload(owned_wave, monkeypatch):
    """A transport that fails to apply the path cannot expose a borrowed writer."""
    state, database, *_ = owned_wave
    original_execute = Driver.execute

    async def execute(self, statement, *parameters):
        if statement == custody._SET_SETTING and parameters[0] == "search_path":
            return "SELECT 1"
        return await original_execute(self, statement, *parameters)

    monkeypatch.setattr(Driver, "execute", execute)
    with pytest.raises(custody.OwnedWalTransactionError) as caught:
        async with custody.registry_owned_wal_transaction(database):
            pytest.fail("unverified search path reached the payload")
    outcome = caught.value.outcome
    assert str(caught.value.__cause__) == "provider_directory_owned_wal_settings_changed"
    assert outcome.commit_state == "not_attempted" and outcome.measurement is None
    assert outcome.cleanup_complete and state.connections[0].invalidated
    assert "preflight" not in state.connections[0].events
    assert "begin" not in state.connections[0].events


@pytest.mark.asyncio
async def test_owner_path_restore_failure_preserves_commit_and_retires_connection(owned_wave, monkeypatch):
    """A failed path restore cannot return a contaminated connection to the pool."""
    state, database, *_ = owned_wave
    original_execute = Driver.execute

    async def execute(self, statement, *parameters):
        if statement == custody._SET_SETTING and parameters[0] == "search_path" and "commit" in self.connection.events:
            raise OSError("synthetic search path restore failure")
        return await original_execute(self, statement, *parameters)

    monkeypatch.setattr(Driver, "execute", execute)
    with pytest.raises(custody.OwnedWalTransactionError) as caught:
        async with custody.registry_owned_wal_transaction(database) as outcome:
            assert outcome.session.in_transaction()
    outcome = caught.value.outcome
    assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    assert outcome.measurement is None and outcome.cleanup_complete
    assert state.connections[0].closed and state.connections[0].invalidated


@pytest.mark.parametrize("phase", ["start", "raw_capture"])
async def test_failed_checkout_or_raw_capture_retains_unknown_native_custody(owned_wave, monkeypatch, phase):
    """Never report uncaptured native cleanup or fetch a replacement driver."""
    state, database, *_ = owned_wave
    primary = OSError("synthetic retained connection preparation failure")
    original_connect = database.engine.connect

    def connect():
        connection = original_connect()
        connection.get_raw_connection = AsyncMock(side_effect=primary)
        connection.invalidate = AsyncMock()
        connection.close = AsyncMock()
        if phase == "start":
            connection.start = AsyncMock(side_effect=primary)
        return connection

    database.engine.connect = connect
    with pytest.raises(custody.OwnedWalTransactionError) as caught:
        async with custody.registry_owned_wal_transaction(database):
            pytest.fail("uncaptured native transaction entered")
    outcome = caught.value.outcome
    assert caught.value.__cause__ is primary
    assert not outcome.cleanup_complete and outcome.commit_state == "not_attempted"
    assert outcome.measurement is None and outcome.session is None
    assert outcome.retained_driver is None and outcome.retained_pid is None
    assert len(state.connections) == 1 and outcome.connection is state.connections[0]
    connection = outcome.connection
    connection.invalidate.assert_not_called()
    connection.close.assert_not_called()
    assert not connection.closed and not connection.driver.is_closed()
    if phase == "start":
        connection.get_raw_connection.assert_not_called()
        assert connection.sync_connection is None and state.checkedout == 0
    else:
        connection.get_raw_connection.assert_awaited_once()
        assert connection.sync_connection is not None and state.checkedout == 1
        await Connection.invalidate(connection)
        await Connection.close(connection)
    database.connect.assert_not_awaited()
    database.session_factory.assert_not_called()


async def test_worker_invalidated_connection_never_fetches_replacement_or_commits(owned_wave):
    """Lost wrapper custody stays uncertain and preserves the original endpoint."""
    state, database, *_ = owned_wave
    with pytest.raises(custody.OwnedWalTransactionError) as caught:
        async with custody.registry_owned_wal_transaction(database) as outcome:
            connection = outcome.connection
            original_driver, original_pid = outcome.retained_driver, outcome.retained_pid
            await connection.invalidate()
            connection.get_raw_connection = AsyncMock(side_effect=AssertionError("replacement driver forbidden"))
    assert caught.value.outcome is outcome
    assert str(caught.value.__cause__) == "provider_directory_owned_wal_custody_changed"
    assert outcome.retained_driver is original_driver and outcome.retained_pid == original_pid
    assert outcome.commit_state == "uncertain" and not outcome.is_committed
    assert outcome.measurement is None and outcome.cleanup_complete
    connection.get_raw_connection.assert_not_called()
    assert "commit" not in connection.events and len(state.connections) == 1
    assert connection.closed and state.checkedout == 0
    database.connect.assert_not_awaited()
    database.session_factory.assert_not_called()
