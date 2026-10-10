# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual retained owner coupled to the original maintenance release boundary."""

from __future__ import annotations

import ast
import asyncio
import contextvars
import dataclasses
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy import event, text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.pool import QueuePool

from db.connection import Database
from process import provider_directory_owned_wal_transaction as owner
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_control_custody as control_custody
from process import provider_directory_profile_control_maintenance as maintenance
from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticUnavailable
from process.provider_directory_profile_capacity_types import BOUNDED_ADMISSION_CONTRACT_ID
from tests.test_provider_directory_owned_evidence_wave import Connection, Driver, Session
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave
from tests.test_provider_directory_profile_control_maintenance import fixture

pytestmark = pytest.mark.asyncio


class NativeBegin:
    """Model the native begin API returning a non-coroutine awaitable."""

    def __init__(self, connection):
        self.connection = connection

    def __await__(self):
        return self.connection.begin_transaction().__await__()


class Retained(Connection, AsyncConnection):
    """Use the existing real diagnostic driver fixture on one retained checkout."""

    closed = invalidated = False
    sync_connection = dialect = None

    def __init__(self, mode):
        state = SimpleNamespace(mode=mode, pool=asyncio.Semaphore(1), checkedout=0, max_checkedout=0)
        Connection.__init__(self, state, 0)
        self.dialect = SimpleNamespace(name="postgresql", driver="asyncpg")
        self.commit_gate = self.rollback_gate = None
        self.commit_started = asyncio.Event()
        self.rollback_started = asyncio.Event()
        self.commit_failure = None
        self.has_nested_transaction = False
        self.driver.terminate = self.terminate_driver

    def terminate_driver(self):
        self.events.append("native_terminate")
        self.driver.closed = True
        self.driver.in_transaction = False

    def in_transaction(self):
        return self.driver.in_transaction

    def in_nested_transaction(self):
        return self.has_nested_transaction

    def begin(self):
        return NativeBegin(self)

    async def begin_transaction(self):
        self.events.append("begin")
        self.driver.in_transaction = True

    async def execute(self, statement, parameters=None):
        assert self.driver.in_transaction
        self.events.append((str(statement), parameters))
        return SimpleNamespace(scalar_one=lambda: 5)

    async def commit(self):
        self.events.append("commit")
        self.commit_started.set()
        if self.commit_gate is not None:
            await self.commit_gate.wait()
        self.driver.in_transaction = False
        if self.commit_failure is not None:
            raise self.commit_failure

    async def rollback(self):
        self.events.append("rollback")
        self.rollback_started.set()
        if self.rollback_gate is not None:
            await self.rollback_gate.wait()
        self.driver.in_transaction = False


def _install_callback_owner(fhir):
    """Use the actual separate custody owner for the original identity callback."""
    callback_state = SimpleNamespace(
        mode="success", pool=asyncio.Semaphore(1), checkedout=0, max_checkedout=0, connections=[]
    )
    original_pool = fhir.db.engine.pool
    callback_database = Database()
    callback_database._database_override = "synthetic"

    def callback_connect():
        connected = Connection(callback_state, len(callback_state.connections) + 1)
        callback_state.connections.append(connected)
        return connected

    callback_database.engine = SimpleNamespace(
        connect=callback_connect, pool=original_pool, dialect=SimpleNamespace(name="postgresql", driver="asyncpg")
    )
    callback_database.session_factory = Mock(side_effect=AssertionError("no replacement callback owner"))
    fhir.db = callback_database
    original_transaction = fhir._provider_directory_profile_capacity_transaction

    @asynccontextmanager
    async def callback_transaction(*, control_identity=None):
        with patch.object(owner, "AsyncSession", Session):
            async with control_custody.transaction(
                fhir, original_transaction(), identity=control_identity, enabled=control_identity is not None
            ):
                yield

    fhir._provider_directory_profile_capacity_transaction = callback_transaction
    fhir.callback_state = callback_state


async def coupled_fixture(mode="success"):
    """Connect original maintenance SQL to the actual retained owner candidate."""
    fhir, _, events, admission = fixture()
    admission.geometry = dataclasses.replace(
        admission.geometry, physical_projection_contract_id=BOUNDED_ADMISSION_CONTRACT_ID
    )
    projection = capacity.project_profile_control_wal_capacity(
        admission.geometry, admission.control_wal_projection.plan_input
    )
    admission.geometry = dataclasses.replace(
        admission.geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    admission.control_wal_projection = capacity.project_profile_control_wal_capacity(
        admission.geometry, projection.plan_input
    )
    admission.wal_tracker = SimpleNamespace(owned_control_maintenance_outcome=None, owned_control_transaction_groups=[])
    connection = Retained(mode)
    await connection.start()
    captures = []
    fhir.profile_owned_wal = owner
    fhir.profile_control_custody = control_custody
    fhir._PROFILE_CAPACITY_MUTATION_WINDOW = contextvars.ContextVar("synthetic_maintenance_window", default=None)
    original_window = fhir._profile_capacity_mutation_window

    @asynccontextmanager
    async def observed_window(relation):
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), relation))
        try:
            async with original_window(relation):
                yield
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)

    fhir._profile_capacity_mutation_window = observed_window
    fhir._profile_owned_transaction_capture = lambda actual, failure: captures.append((actual, failure))
    _install_callback_owner(fhir)

    async def guard(source, expected):
        assert source is admission.admitted_identity and expected is admission.database_identity
        assert connection.in_transaction()
        events.append("admitted_identity_verified")

    fhir._admission_database_guard = guard
    return fhir, connection, events, admission, captures


async def close_healthy(connection):
    """Return only a healthy restored transport after assertions."""
    if not connection.closed:
        await connection.close()
    assert connection.state.checkedout == 0


async def test_native_begin_once_held_until_original_release_and_capture_before_settlement():
    fhir, connection, events, admission, captures = await coupled_fixture()
    async with maintenance.control_maintenance_fence(fhir, connection):
        assert connection.events.count("begin") == 1 and "commit" not in connection.events
        assert admission.wal_tracker.owned_control_maintenance_outcome is None
        await maintenance.assert_held(fhir)
        await maintenance.release_before_cutover(fhir)
        outcome = admission.wal_tracker.owned_control_maintenance_outcome
        assert outcome.is_committed and outcome.measurement is not None and outcome.cleanup_complete
        assert outcome.connection is connection and outcome.retained_driver is connection.driver
        assert outcome.retained_pid == connection.driver.pid
        assert captures == [(outcome, None)] and events[-1] == "window_settle"
        assert not connection.closed and connection.driver.settings == connection.driver.original
    assert connection.events.count("commit") == 1 and "rollback" not in connection.events
    await close_healthy(connection)


async def test_body_cancellation_rolls_back_and_drains_once_preserving_primary_identity():
    fhir, connection, _, admission, captures = await coupled_fixture()
    primary = asyncio.CancelledError()
    connection.rollback_gate = asyncio.Event()

    async def run():
        async with maintenance.control_maintenance_fence(fhir, connection):
            raise primary

    task = asyncio.create_task(run())
    await connection.rollback_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not connection.closed
    connection.rollback_gate.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value is primary and outcome.commit_state == "rolled_back"
    assert connection.events.count("rollback") == 1 and "commit" not in connection.events
    assert outcome.cleanup_complete and captures[0][0] is outcome
    assert connection.closed and connection.state.checkedout == 0


@pytest.mark.parametrize("cause", ["cancel", "timeout"])
async def test_release_interruption_drains_commit_and_retains_confirmed_unmeasured_truth(cause):
    fhir, connection, _, admission, captures = await coupled_fixture()
    connection.commit_gate = asyncio.Event()
    fhir._profile_capacity_remaining_ms = AsyncMock(return_value=10 if cause == "timeout" else 1000)

    async def run():
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.release_before_cutover(fhir)

    task = asyncio.create_task(run())
    await connection.commit_started.wait()
    if cause == "cancel":
        task.cancel()
        await asyncio.sleep(0)
    else:
        await asyncio.sleep(0.02)
    assert not task.done() and not connection.closed
    connection.commit_gate.set()
    expected = asyncio.CancelledError if cause == "cancel" else TimeoutError
    with pytest.raises(expected):
        await task
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert outcome.is_committed and outcome.measurement is None and outcome.cleanup_complete
    assert outcome.status == "committed_accounting_incomplete"
    assert connection.events.count("commit") == 1 and "rollback" not in connection.events
    assert connection.closed and captures[0][0] is outcome
    assert connection.state.checkedout == 0


@pytest.mark.parametrize("mode", ["unknown_commit", "postcommit_sample", "persistent_retirement"])
async def test_failed_release_retains_genuine_outcome_and_exact_transport_quarantine(mode):
    fhir, connection, _, admission, captures = await coupled_fixture()
    primary = OSError("synthetic commit acknowledgement loss")
    if mode in {"unknown_commit", "persistent_retirement"}:
        connection.commit_failure = primary
    if mode == "persistent_retirement":
        connection.invalidate = AsyncMock(side_effect=OSError("synthetic retirement loss"))
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            if mode == "postcommit_sample":
                connection.driver.fail_command = "sample"
                connection.driver.error = primary
            await maintenance.release_before_cutover(fhir)
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value.outcome is outcome
    cause = caught.value.__cause__
    if mode == "postcommit_sample":
        assert isinstance(cause, BackendWalDiagnosticUnavailable)
    else:
        assert cause is primary
    assert captures[0][0] is outcome and outcome.measurement is None
    assert outcome.retained_driver is connection.driver and outcome.retained_pid == connection.driver.pid
    assert outcome.is_committed is (mode == "postcommit_sample")
    if mode == "persistent_retirement":
        assert not outcome.cleanup_complete and not connection.closed and not connection.invalidated
        assert connection.state.checkedout == 1
        connection.execute = AsyncMock()
        commit_count = connection.events.count("commit")
        await release_helper()(connection, "synthetic-build", maintenance_admission=admission)
        connection.execute.assert_not_called()
        assert connection.events.count("commit") == commit_count and "pool_return" not in connection.events
        await Connection.invalidate(connection)
        await Connection.close(connection)
    else:
        assert outcome.cleanup_complete and connection.closed
    assert connection.state.checkedout == 0


def release_helper():
    """Load the exact teardown definition without importing unrelated runtime."""
    source = Path(maintenance.__file__).with_name("provider_directory_fhir.py")
    tree = ast.parse(source.read_text())
    definition = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_release_provider_directory_artifact_build_lock"
    )
    globals_by_name = {"Any": object, "sa_text": text}
    exec(compile(ast.Module(body=[definition], type_ignores=[]), str(source), "exec"), globals_by_name)
    return globals_by_name[definition.name]


async def test_sqlalchemy_discard_without_native_driver_termination_must_not_claim_cleanup_complete():
    fhir, connection, _, admission, _ = await coupled_fixture()
    primary = OSError("synthetic commit acknowledgement loss")
    connection.commit_failure = primary

    async def discard_without_termination():
        connection.invalidated = True
        connection.events.append("invalidate_without_driver_termination")

    connection.invalidate = discard_without_termination
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.release_before_cutover(fhir)
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value.__cause__ is primary and outcome.retained_driver is connection.driver
    assert connection.driver.is_closed()
    assert outcome.cleanup_complete
    assert connection.events.index("native_terminate") < connection.events.index(
        "invalidate_without_driver_termination"
    )


@pytest.mark.parametrize("failure", ["raises", "returns_live"])
async def test_native_termination_failure_quarantines_without_pool_invalidation(failure):
    fhir, connection, _, admission, captures = await coupled_fixture()
    primary = OSError("synthetic commit acknowledgement loss")
    connection.commit_failure = primary

    def terminate():
        connection.events.append("native_terminate_attempt")
        if failure == "raises":
            raise OSError("synthetic native termination failure")

    connection.driver.terminate = terminate
    connection.invalidate = AsyncMock()
    connection.close = AsyncMock()
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.release_before_cutover(fhir)
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value.__cause__ is primary and caught.value.outcome is outcome
    assert captures[0][0] is outcome and not outcome.cleanup_complete
    assert outcome.retained_driver is connection.driver and outcome.retained_pid == connection.driver.pid
    assert not connection.driver.is_closed() and connection.state.checkedout == 1
    connection.invalidate.assert_not_called()
    connection.close.assert_not_called()
    connection.execute = AsyncMock()
    await release_helper()(connection, "synthetic-build", maintenance_admission=admission)
    connection.execute.assert_not_called()
    connection.close.assert_not_called()
    await Connection.invalidate(connection)
    await Connection.close(connection)


async def test_installed_sqlalchemy_suppressed_pool_termination_checks_in_only_closed_driver():
    fhir, connection, _, admission, _ = await coupled_fixture()
    dialect = DefaultDialect()
    dialect.do_terminate = Mock(side_effect=OSError("synthetic DBAPI termination failure"))
    pool = QueuePool(lambda: connection.driver, pool_size=1, max_overflow=0, reset_on_return=None, dialect=dialect)
    checkout = pool.connect()
    checkins = []
    event.listen(pool, "checkin", lambda *_: checkins.append(connection.driver.is_closed()))

    async def invalidate():
        checkout.invalidate()
        connection.invalidated = True

    connection.invalidate = invalidate
    primary = OSError("synthetic commit acknowledgement loss")
    connection.commit_failure = primary
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.release_before_cutover(fhir)
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value.__cause__ is primary
    assert outcome.cleanup_complete and connection.driver.is_closed()
    assert checkins == [True] and pool.checkedout() == connection.state.checkedout == 0
    dialect.do_terminate.assert_called_once_with(connection.driver)
    assert connection.events.count("native_terminate") == 1
    pool.dispose()


@pytest.mark.parametrize("is_closed", [False, True])
async def test_installed_asyncpg_disconnect_predicate_uses_original_native_terminal_state(is_closed):
    driver = SimpleNamespace(is_closed=lambda: is_closed)
    wrapper = SimpleNamespace(_connection=driver)
    assert PGDialect_asyncpg().is_disconnect(OSError("synthetic rollback failure"), wrapper, None) is is_closed


@pytest.mark.parametrize("kind", ["retained", "worker"])
async def test_restore_and_termination_dual_failure_preserves_first_cleanup_exception(kind, owned_wave, monkeypatch):
    state, database, *_ = owned_wave
    primary = OSError("synthetic first settings restore failure")
    retirement = OSError("synthetic second native termination failure")
    original_execute = Driver.execute

    async def execute(driver, statement, *parameters):
        target_owner = kind == "worker" or isinstance(driver.connection, Retained)
        if target_owner and statement == owner._SET_SETTING and "commit" in driver.connection.events:
            raise primary
        return await original_execute(driver, statement, *parameters)

    def terminate(driver):
        raise retirement

    monkeypatch.setattr(Driver, "execute", execute)
    monkeypatch.setattr(Driver, "terminate", terminate)
    if kind == "retained":
        fhir, connection, _, admission, _ = await coupled_fixture()
        connection.driver.terminate = lambda: terminate(connection.driver)
        context = maintenance.control_maintenance_fence(fhir, connection)
    else:
        context = owner.registry_owned_wal_transaction(database)
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with context as active_outcome:
            assert connection.in_transaction() if kind == "retained" else active_outcome.session.in_transaction()
    outcome = caught.value.outcome
    connection = outcome.connection
    assert caught.value.__cause__ is primary
    assert "provider_directory_owned_wal_retirement_incomplete" in primary.__notes__
    assert outcome.is_committed and outcome.measurement is None and not outcome.cleanup_complete
    assert outcome.retained_driver is connection.driver and outcome.retained_pid == connection.driver.pid
    assert not connection.closed and not connection.driver.is_closed()
    assert "invalidate" not in connection.events and "pool_return" not in connection.events
    await Connection.invalidate(connection)
    await Connection.close(connection)


async def test_worker_installed_pool_suppression_checks_in_only_verified_terminal_driver(owned_wave, monkeypatch):
    state, database, *_ = owned_wave
    state.mode = "unknown_commit"
    original_connect = database.engine.connect
    checkins = []
    pools = []

    def connect():
        connection = original_connect()
        dialect = DefaultDialect()
        dialect.do_terminate = Mock(side_effect=OSError("synthetic DBAPI termination failure"))
        pool = QueuePool(lambda: connection.driver, pool_size=1, max_overflow=0, reset_on_return=None, dialect=dialect)
        checkout = pool.connect()
        event.listen(pool, "checkin", lambda *_: checkins.append(connection.driver.is_closed()))
        pools.append(pool)

        async def invalidate():
            checkout.invalidate()
            connection.invalidated = True

        connection.invalidate = invalidate
        return connection

    database.engine.connect = connect
    with pytest.raises(owner.OwnedWalTransactionError) as caught:
        async with owner.registry_owned_wal_transaction(database) as active_outcome:
            assert active_outcome.session.in_transaction() and active_outcome.retained_driver is not None
    outcome = caught.value.outcome
    assert outcome.cleanup_complete and outcome.retained_driver.is_closed()
    assert outcome.retained_pid == outcome.retained_driver.pid
    assert checkins == [True] and state.checkedout == 0 and pools[0].checkedout() == 0
    assert state.connections[0].events.count("native_terminate") == 1
    pools[0].dispose()
