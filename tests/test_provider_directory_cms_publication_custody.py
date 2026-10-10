# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual CMS owner routes preserve native factory, locks, settings and commit truth."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_cutover_custody import install_existing_savepoints
from tests.test_provider_directory_owned_evidence_wave import Connection, Session, custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

publication = importlib.import_module("process.provider_directory_cms_publication")
capture = importlib.import_module("process.network_registry_cms_capture_lock")
pytestmark = pytest.mark.asyncio
CAPTURE_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
RECEIPT_BY_SECTION = {
    "cms": {"dataset_id": "synthetic"},
    "profile": {"generation_id": "synthetic"},
    "address": {"local_generation": 1},
    "doctors": {"local_generation": 1},
}


class SourceConnection(Connection):
    def __init__(self, state, ordinal, engine):
        super().__init__(state, ordinal)
        self.engine, self.isolation, self.locked = engine, None, False
        self.driver.settings = dict(zip(custody._SETTINGS, ("0", "0", "0", "off", '"synthetic_source"')))
        self.driver.original = dict(self.driver.settings)

    def in_transaction(self):
        return self.driver.in_transaction

    async def execution_options(self, *, isolation_level):
        self.isolation = isolation_level
        self.events.append("isolation:" + isolation_level)
        return self

    async def scalar(self, statement, params):
        assert not self.in_transaction() and self.isolation == "AUTOCOMMIT"
        if "pg_try_advisory_lock" in str(statement):
            self.locked = True
            self.events.append("capture_lock")
        else:
            assert "pg_advisory_unlock" in str(statement) and self.locked
            self.locked = False
            self.events.append("capture_unlock")
        is_successful = True
        return is_successful

    async def commit(self):
        self.events.append("autocommit_end")

    async def invalidate(self):
        self.locked = False
        await super().invalidate()


class OriginalTransaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        await Session.begin(self.session)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.session.commit()
        else:
            await self.session.rollback()
        return False


class SourceSession(Session):
    info = None

    def __init__(self, connection, *, owns_connection=False):
        super().__init__(bind=connection, expire_on_commit=False, autoflush=False)
        self.owns_connection, self.info = owns_connection, {"actor": "synthetic_source_actor"}

    async def __aenter__(self):
        if self.owns_connection:
            await self.bind.start()
        return self

    async def __aexit__(self, *args):
        await self.close()
        if self.owns_connection:
            await self.bind.close()
        return False

    def begin(self):
        return OriginalTransaction(self)

    async def execute(self, statement, parameters=None):
        if "limits" not in self.bind.events:
            self.bind.events.append("limits")
        return await super().execute(statement, parameters or {})


@pytest.fixture
def source_wave(control_wave, monkeypatch):
    """Retain the original factory and pool-one source connection around native transactions."""
    state, database, admission, *_ = control_wave
    engine = SimpleNamespace(pool=SimpleNamespace(checkedout=lambda: state.checkedout))

    def connect():
        connection = SourceConnection(state, len(state.connections), engine)
        state.connections.append(connection)
        return connection

    engine.connect = connect
    calls = []

    def factory(*, bind=None):
        calls.append(bind)
        return SourceSession(bind or connect(), owns_connection=bind is None)

    monkeypatch.setattr(capture, "_source_engine", lambda original, capture_id: engine if original is factory else None)
    return state, database, admission, factory, calls


def source_group(fixture):
    return fixture[2].wal_tracker.owned_control_transaction_groups[0]


async def write_publication(fixture, *, capture_id=CAPTURE_ID):
    _, database, _, factory, _ = fixture
    async with publication._publication_session(fhir, factory, capture_id=capture_id) as session:
        assert session.info == {"actor": "synthetic_source_actor"}
        assert session is database._transaction_binding().session
        await database.status("UPDATE synthetic_target SET generation_id='new';")


async def test_capture_route_retains_original_factory_native_owner_lock_and_settings(source_wave):
    state, database, _, factory, calls = source_wave
    await write_publication(source_wave)
    group = source_group(source_wave)
    outcome = group["original_outcome"]
    assert group["identity"][0] == "cms_publication_source"
    assert group["identity"][1] is factory and group["identity"][2] is CAPTURE_ID
    assert group["identity"][3] is outcome.session and calls == [state.connections[0]]
    assert outcome.connection is state.connections[0] and outcome.retained_driver is state.connections[0].driver
    assert group["consumed"] and outcome.is_committed and outcome.status == "committed_measured"
    assert outcome.cleanup_complete and database._transaction_binding() is None
    assert not group["accounting_authority"] and not group["reservation_refund"]
    connection = state.connections[0]
    assert connection.events.count("begin") == connection.events.count("commit") == 1
    assert connection.events.index("capture_lock") < connection.events.index("begin")
    assert (
        connection.events.index("commit")
        < connection.events.index("capture_unlock")
        < connection.events.index("pool_return")
    )
    assert not connection.locked and connection.driver.settings == connection.driver.original


@pytest.mark.parametrize("mode", ["rollback", "cancel", "unknown_commit"])
async def test_capture_failure_keeps_original_native_truth_and_retires_original_lock(source_wave, mode):
    state, _, _, _, _ = source_wave
    state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception) as caught:
        await write_publication(source_wave)
    group = source_group(source_wave)
    outcome = group["original_outcome"]
    assert group["failure"] is caught.value and not group["consumed"]
    assert outcome.commit_state == ("attempted" if mode == "unknown_commit" else "rolled_back")
    assert outcome.cleanup_complete and outcome.connection.invalidated and outcome.connection.closed
    assert not outcome.connection.locked and state.checkedout == 0


async def test_failed_final_measurement_does_not_replace_original_committed_publication(source_wave):
    state, _, _, _, _ = source_wave
    state.mode = "postcommit_sample"
    await write_publication(source_wave)
    group = source_group(source_wave)
    outcome = group["original_outcome"]
    assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    assert outcome.cleanup_complete and not group["consumed"] and group["failure"] is None
    assert "capture_unlock" in outcome.connection.events and not outcome.connection.invalidated


async def test_incapable_original_settings_are_held_without_mutating_source_factory(source_wave):
    state, _, _, _, _ = source_wave
    engine = capture._source_engine(source_wave[3], CAPTURE_ID)
    connect = engine.connect

    def nonparallel_unproved():
        connection = connect()
        connection.driver.preflight["parallel_workers"] = 8
        connection.driver.settings["max_parallel_workers"] = "8"
        connection.driver.original = dict(connection.driver.settings)
        return connection

    engine.connect = nonparallel_unproved
    await write_publication(source_wave)
    group = source_group(source_wave)
    outcome = group["original_outcome"]
    assert outcome.is_committed and outcome.measurement is None
    assert outcome.status == "committed_accounting_incomplete" and not group["consumed"]
    assert outcome.connection.driver.settings["max_parallel_workers"] == "8"
    assert outcome.connection.driver.settings == outcome.connection.driver.original
    assert not outcome.connection.invalidated and state.checkedout == 0


@pytest.mark.parametrize("mode", ["success", "rollback", "cancel", "unknown_commit"])
async def test_ordinary_callable_factory_keeps_original_boundary_and_unknown_native_custody(source_wave, mode):
    state, _, _, factory, calls = source_wave
    state.mode = mode
    if mode == "success":
        await write_publication(source_wave, capture_id=None)
    else:
        with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception) as caught:
            await write_publication(source_wave, capture_id=None)
    group = source_group(source_wave)
    assert group["identity"][1] is factory and calls == [None]
    assert group["original_outcome"] is None and group["outcome"] is None and not group["consumed"]
    assert group["failure"] is (None if mode == "success" else caught.value)
    assert state.connections[0].events.count("begin") == 1 and state.checkedout == 0


async def test_default_database_route_uses_genuine_existing_native_owner(control_wave):
    state, database, admission, *_ = control_wave
    async with publication._publication_session(fhir, None) as session:
        assert session is database._transaction_binding().session
        await fhir._apply_provider_directory_profile_capacity_settings(admission)
        await database.status("UPDATE synthetic_target SET generation_id='new';")
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["identity"][1] == "cms_default_pool" and group["consumed"]
    assert group["original_outcome"].is_committed and group["original_outcome"].cleanup_complete
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


@pytest.mark.parametrize("cancelled", [False, True])
async def test_borrowed_default_owner_keeps_existing_savepoint_and_cancellation(control_wave, monkeypatch, cancelled):
    state, database, admission, *_ = control_wave
    install_existing_savepoints(monkeypatch, database)
    if cancelled:
        state.mode = "cancel"
    try:
        async with custody.registry_owned_wal_transaction(database) as outcome:
            async with publication._publication_session(fhir, None) as session:
                assert session.in_nested_transaction()
                await fhir._apply_provider_directory_profile_capacity_settings(admission)
                await database.status("UPDATE synthetic_target SET generation_id='new';")
            group = admission.wal_tracker.owned_control_transaction_groups[0]
            assert group["original_outcome"] is outcome and not group["consumed"]
    except asyncio.CancelledError:
        assert cancelled
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert not group["consumed"] and state.checkedout == 0
    assert (
        state.connections[0].events.count("savepoint_begin") == state.connections[0].events.count("savepoint_end") == 1
    )
    assert outcome.commit_state == ("rolled_back" if cancelled else "confirmed")


async def test_actual_prepared_results_consumes_actual_bundle_helper_inside_source_owner(source_wave, monkeypatch):
    _, database, _, factory, _ = source_wave
    stage = SimpleNamespace(schema="synthetic", stage_table="scratch", target_relation="target", build_fence=None)
    prepared = SimpleNamespace(stages=(stage,), profile_delta=None, nonprofile_admission=None)
    monkeypatch.setattr(publication.coverage, "assert_sealed_cms_candidate_coverage", AsyncMock())
    monkeypatch.setattr(publication, "_seal_published_coverage", AsyncMock())
    monkeypatch.setattr(publication, "_lock_live_swap_relations", AsyncMock())
    monkeypatch.setattr(publication, "_receipt_payload", Mock(return_value={"synthetic": True}))
    monkeypatch.setattr(
        publication.receipts, "capture_native_dependencies", AsyncMock(return_value={"synthetic": True})
    )
    monkeypatch.setattr(publication.receipts, "append_serving_receipt", AsyncMock(return_value="synthetic_receipt"))
    monkeypatch.setattr(publication, "_bind_registry_source_receipt", AsyncMock())
    monkeypatch.setattr(fhir, "_ordered_provider_directory_artifact_bundle", lambda stages: stages)
    monkeypatch.setattr(
        fhir, "_provider_directory_artifact_bundle_context", lambda *_: ("synthetic", ("target",), "1s", "2s")
    )
    monkeypatch.setattr(fhir.profile_initial, "build_from_stages", Mock(return_value=None))
    monkeypatch.setattr(fhir.profile_initial, "lock_metadata", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_provider_directory_artifact_bundle_targets", AsyncMock())
    monkeypatch.setattr(fhir, "_reserve_provider_directory_artifact_cutover_budget", AsyncMock(return_value=None))

    async def apply(stages, schema, relations, profile_delta, active_fence, timeout, *, before_swaps):
        assert custody.current_owned_wal_transaction(database) is not None
        await before_swaps()
        await database.status("UPDATE synthetic_target SET generation_id='new';")

    monkeypatch.setattr(fhir, "_apply_locked_provider_directory_artifact_bundle", apply)
    async with publication._publication_session(fhir, factory, capture_id=CAPTURE_ID):
        assert await publication._apply_prepared_results(fhir, object(), prepared, None, object(), None, None) == (
            "synthetic_receipt",
            {"synthetic": True},
        )
    group = source_group(source_wave)
    assert group["consumed"] and group["original_outcome"].is_committed
    assert group["original_outcome"].connection.events.count("begin") == 1


@pytest.mark.parametrize("is_cancelled", [False, True])
async def test_capture_retirement_preserves_body_failure(source_wave, monkeypatch, is_cancelled):
    state, _, _, factory, _ = source_wave
    failure = asyncio.CancelledError("synthetic cancellation") if is_cancelled else RuntimeError("synthetic body")

    def fail_termination(_driver):
        raise OSError("synthetic retirement failure")

    original_terminate = custody._terminate_owned_driver
    monkeypatch.setattr(custody, "_terminate_owned_driver", fail_termination)
    with pytest.raises(type(failure)) as caught:
        async with capture.registry_cms_capture_transaction(factory, capture_id=CAPTURE_ID, fhir=fhir):
            raise failure
    connection = state.connections[0]
    try:
        assert caught.value is failure
        assert "registry_cms_capture_cleanup_incomplete" in failure.__notes__
        assert "invalidate" not in connection.events and "pool_return" not in connection.events
        assert not connection.driver.is_closed() and not connection.closed
    finally:
        monkeypatch.setattr(custody, "_terminate_owned_driver", original_terminate)
        await custody._retire_owned_connection(connection, connection.driver, custody.OwnedWalTransaction())


async def test_capture_failure_terminates_before_checkin(source_wave):
    state, _, _, factory, _ = source_wave
    failure = asyncio.CancelledError("synthetic cancellation")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with capture.registry_cms_capture_transaction(factory, capture_id=CAPTURE_ID, fhir=fhir):
            raise failure
    assert caught.value is failure
    connection = state.connections[0]
    assert connection.events.index("native_terminate") < connection.events.index("pool_return")
    assert connection.driver.is_closed() and connection.closed and state.checkedout == 0


async def test_unlock_failure_retires_exact_driver(source_wave, monkeypatch):
    state, _, _, factory, _ = source_wave
    failure = OSError("synthetic unlock failure")
    monkeypatch.setattr(capture, "_unlock_capture", AsyncMock(side_effect=failure))
    with pytest.raises(OSError) as caught:
        async with capture.registry_cms_capture_transaction(factory, capture_id=CAPTURE_ID) as session:
            assert session is not None
    assert caught.value is failure
    connection = state.connections[0]
    assert connection.events.index("native_terminate") < connection.events.index("pool_return")
    assert connection.driver.is_closed() and state.checkedout == 0


async def test_start_cancellation_retains_exact_driver(source_wave, monkeypatch):
    state, _, _, factory, _ = source_wave
    entered, release = asyncio.Event(), asyncio.Event()
    original_start = SourceConnection.start

    async def delayed_start(connection):
        await original_start(connection)
        entered.set()
        await release.wait()
        return connection

    monkeypatch.setattr(SourceConnection, "start", delayed_start)

    async def invoke():
        async with capture.registry_cms_capture_transaction(factory, capture_id=CAPTURE_ID):
            raise AssertionError("cancelled start must not yield")

    task = asyncio.create_task(invoke())
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection = state.connections[0]
    assert connection.events.index("native_terminate") < connection.events.index("pool_return")
    assert connection.driver.is_closed() and connection.closed and state.checkedout == 0
    assert "begin" not in connection.events


async def test_close_cancellation_does_not_retire_returned_driver(source_wave, monkeypatch):
    state, _, _, factory, _ = source_wave
    entered, release = asyncio.Event(), asyncio.Event()
    original_close = SourceConnection.close

    async def delayed_close(connection):
        entered.set()
        await release.wait()
        await original_close(connection)

    monkeypatch.setattr(SourceConnection, "close", delayed_close)

    async def invoke():
        async with capture.registry_cms_capture_transaction(factory, capture_id=CAPTURE_ID) as session:
            assert session is not None

    task = asyncio.create_task(invoke())
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection = state.connections[0]
    assert connection.events.count("pool_return") == 1 and "native_terminate" not in connection.events
    assert not connection.driver.is_closed() and connection.closed and state.checkedout == 0


async def test_commit_cancellation_recovers_receipt_despite_retirement_error(source_wave, monkeypatch):
    state, database, _, factory, _ = source_wave
    entered, release = asyncio.Event(), asyncio.Event()
    original_commit = SourceSession.commit
    original_terminate = custody._terminate_owned_driver
    prepared = SimpleNamespace(source_session_factory=factory, metrics={"cms_serving": {}}, mark_committed=AsyncMock())

    async def delayed_commit(session):
        entered.set()
        await release.wait()
        await original_commit(session)

    def failed_termination(_driver):
        raise OSError("synthetic retirement failure")

    async def apply(*_args):
        await database.status("UPDATE synthetic_target SET generation_id='new';")
        return "synthetic_receipt", RECEIPT_BY_SECTION

    async def is_verified(_fhir, receipt_id, payload):
        assert _fhir is fhir and receipt_id == "synthetic_receipt" and payload is RECEIPT_BY_SECTION
        assert state.connections[0].events.count("commit") == 1
        assert source_group(source_wave)["original_outcome"].is_committed
        return True

    monkeypatch.setattr(SourceSession, "commit", delayed_commit)
    monkeypatch.setattr(custody, "_terminate_owned_driver", failed_termination)
    monkeypatch.setattr(publication, "_assert_address_prepared", lambda *_args: None)

    @asynccontextmanager
    async def transaction(*_args, **_kwargs):
        async with publication._publication_session(fhir, factory, capture_id=CAPTURE_ID) as session:
            yield session, "1s"

    monkeypatch.setattr(publication, "_publication_transaction", transaction)
    monkeypatch.setattr(publication, "_apply_prepared_results", apply)
    monkeypatch.setattr(publication, "_verify_commit", is_verified)
    task = asyncio.create_task(
        publication.commit_prepared_serving_generation(
            fhir, object(), prepared, address=None, candidate_proof=None, native_dependencies=None, predecessor=None
        )
    )
    await entered.wait()
    task.cancel()
    release.set()
    try:
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        prepared.mark_committed.assert_awaited_once_with(profile_result=RECEIPT_BY_SECTION["profile"])
        assert prepared.metrics["cms_serving"]["recovered_commit"] is True
        assert "registry_cms_capture_cleanup_incomplete" in caught.value.__notes__
        assert source_group(source_wave)["failure"] is caught.value
        assert "pool_return" not in state.connections[0].events
    finally:
        monkeypatch.setattr(custody, "_terminate_owned_driver", original_terminate)
        connection = state.connections[0]
        await custody._retire_owned_connection(connection, connection.driver, custody.OwnedWalTransaction())
