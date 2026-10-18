# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host-only retained-owner composition; native role authentication stays external."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_connection, PGDialect_asyncpg

from db.connection import Database
from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_profile_control_custody as control
from process import provider_directory_profile_source_wal as source_wal
from process.provider_directory_profile_source_wal import source_owned_wal_transaction
from tests.test_provider_directory_owned_evidence_wave import Session, fhir
from tests.test_provider_directory_retained_maintenance_coupled import Retained, close_healthy


class RetainedConnection(Retained):
    isolation_level = "READ COMMITTED"
    restore_failure = None

    async def get_isolation_level(self):
        raise AssertionError("wrapper isolation getter starts an unmanaged adapter transaction")

    async def get_raw_connection(self):
        return SimpleNamespace(driver_connection=self.driver, dbapi_connection=self.adapter)

    async def execution_options(self, *, isolation_level):
        assert not self.in_transaction()
        if isolation_level == "READ COMMITTED" and self.isolation_level == "SERIALIZABLE" and self.restore_failure:
            raise self.restore_failure
        self.isolation_level = isolation_level
        PGDialect_asyncpg().set_isolation_level(self.adapter, isolation_level)
        self.events.append(("isolation", isolation_level))
        return self

    async def begin_transaction(self):
        await super().begin_transaction()
        self.top_transaction = object()
        return self.top_transaction

    def get_transaction(self):
        return self.top_transaction if self.in_transaction() else None


class JoinedSession(Session):
    def __init__(self, *, join_transaction_mode, **arguments):
        assert join_transaction_mode == "rollback_only"
        super().__init__(**arguments)

    async def begin(self):
        assert self.bind.in_transaction()
        self.active = True
        self.bind.events.append("session_begin")

    async def connection(self):
        return self.bind

    async def commit(self):
        self.active = False
        self.bind.events.append("session_commit")

    async def close(self):
        self.active = False
        self.bind.events.append("session_close")
        if self.bind.state.mode == "session_close_failure":
            raise OSError("synthetic wrapper close failure")


@pytest.fixture
async def fixture(monkeypatch):
    connection = RetainedConnection("success")
    await connection.start()
    connection.adapter = AsyncAdapt_asyncpg_connection(None, connection.driver)

    async def native_value(statement):
        assert statement == "SHOW transaction_isolation" and not connection.driver.is_in_transaction()
        connection.events.append("native_isolation")
        return "read committed"

    monkeypatch.setattr(connection.driver, "fetchval", native_value, raising=False)
    database = Database()
    database._database_override = "synthetic"
    database.engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql", driver="asyncpg"))
    monkeypatch.setattr(owned, "AsyncSession", JoinedSession)
    try:
        yield database, connection
    finally:
        await close_healthy(connection)


async def test_registered_owner_binds_actual_session_then_commits_after_wrapper_cleanup(fixture):
    database, connection = fixture
    async with owned.registry_retained_wal_transaction(connection) as outcome:
        assert outcome.session is None
        async with owned.bind_retained_wal_transaction(database, outcome) as session:
            assert database._transaction_binding().session is session is outcome.session
            assert database._transaction_binding().borrowed
            assert owned.current_owned_wal_transaction(database) is outcome
            assert outcome.connection is connection and outcome.retained_driver is connection.driver
            assert connection.events.count("begin") == 1 and "commit" not in connection.events
        assert owned.current_owned_wal_transaction(database) is None
        assert database._transaction_binding() is None and connection.in_transaction()
        assert connection.events[-1] == "session_close"
    assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
    assert connection.events.count("commit") == 1
    assert connection.events.index("session_close") < connection.events.index("commit")
    assert connection.driver.settings == connection.driver.original


async def _exercise_owner_fault(database, connection, outcome, fault):
    if fault == "forged":
        outcome = replace(outcome)
    elif fault == "transaction":
        connection.top_transaction = object()
    elif fault == "driver":
        outcome.retained_driver = object()
    elif fault == "connection":
        outcome.connection = object()
    if fault == "child":

        async def child():
            async with owned.bind_retained_wal_transaction(database, outcome):
                pytest.fail("inherited retained owner accepted")

        await asyncio.create_task(child())
    else:
        async with owned.bind_retained_wal_transaction(database, outcome):
            if fault == "session":
                outcome.session = object()
            else:
                pytest.fail("unissued owner accepted")


@pytest.mark.parametrize("fault", ["forged", "child", "transaction", "driver", "connection", "session"])
async def test_forged_or_substituted_owner_refused(fixture, fault):
    database, connection = fixture
    expected = (
        "retained_owner_required"
        if fault in {"forged", "child", "driver", "connection"}
        else "session_changed"
        if fault == "session"
        else "custody_changed"
    )
    with pytest.raises(owned.OwnedWalTransactionError) as caught:
        async with owned.registry_retained_wal_transaction(connection) as outcome:
            await _exercise_owner_fault(database, connection, outcome, fault)
    assert expected in str(caught.value.__cause__)
    assert not caught.value.outcome.is_committed and caught.value.outcome.cleanup_complete
    assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None


@pytest.mark.parametrize("fault", ["body", "cancel", "commit", "close", "session_commit"])
async def test_original_failure_and_uncertain_commit_keep_incomplete_outcome(fixture, fault):
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic cancellation") if fault == "cancel" else ValueError("synthetic body")
    if fault == "commit":
        connection.commit_failure = OSError("synthetic lost commit acknowledgement")
    if fault == "close":
        connection.state.mode = "session_close_failure"
    expected = owned.OwnedWalTransactionCancelled if fault == "cancel" else owned.OwnedWalTransactionError
    with pytest.raises(expected) as caught:
        async with owned.registry_retained_wal_transaction(connection) as outcome:
            async with owned.bind_retained_wal_transaction(database, outcome) as session:
                if fault == "session_commit":
                    await session.commit()
                if fault in {"body", "cancel"}:
                    raise primary
    assert caught.value.outcome is outcome and outcome.cleanup_complete
    assert outcome.measurement is None and not outcome.is_committed
    assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    if fault in {"body", "cancel"}:
        assert caught.value.__cause__ is primary and outcome.commit_state == "rolled_back"
    if fault == "commit":
        assert outcome.commit_state == "attempted"


class IssuedFactory:
    """Declared host transport boundary; supplies no native authentication proof."""

    policy = source.GenericSourcePolicy("fixture", "fixture_owner", "fixture_publisher", ("fixture_reader",))
    producer_identity = ("a" * 40, "sha256:" + "b" * 64)

    def __init__(self, database, connection):
        self.database, self.connection = database, connection
        self.events = []

    def _require_current_policy(self):
        assert source._FACTORY.get() is self

    @asynccontextmanager
    async def checkpoint_guard(self, database):
        assert database is self.database and database._transaction_binding() is None
        self.events.append("guard_enter")
        yield self.connection, None
        assert not self.connection.in_transaction()
        self.events.append("guard_exit")

    @asynccontextmanager
    async def bind_existing_transaction(self, database, session, *, isolation):
        assert database is self.database and isolation in {"read committed", "serializable"}
        assert self.connection.isolation_level == isolation.upper()
        outcome = owned.current_owned_wal_transaction(database)
        assert outcome.session is session and outcome.connection is self.connection
        assert database._transaction_binding().session is session
        self.events.append("source_enter")
        yield
        self.events.append("source_exit")


async def test_source_composition_orders_real_retained_owner_before_issued_binding(fixture):
    database, connection = fixture
    factory = IssuedFactory(database, connection)
    token = source._FACTORY.set(factory)
    try:
        async with source_owned_wal_transaction(SimpleNamespace(db=database)) as outcome:
            assert owned.current_owned_wal_transaction(database) is outcome
            assert outcome.session is database._transaction_binding().session
            assert factory.events == ["guard_enter", "source_enter"]
        assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
        assert factory.events == ["guard_enter", "source_enter", "source_exit", "guard_exit"]
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("body_fails", [False, True])
async def test_control_cleanup_cancellation_keeps_outcome_and_original_cause(fixture, monkeypatch, body_fails):
    database, connection = fixture
    primary = ValueError("synthetic control body failure")
    cleanup_error = asyncio.CancelledError("synthetic control cleanup cancellation")

    async def rollback():
        connection.events.append("rollback")
        raise cleanup_error

    if body_fails:
        monkeypatch.setattr(connection, "rollback", rollback)
    else:
        connection.restore_failure = cleanup_error
    monkeypatch.setattr(control, "is_source_storage_required", lambda: True)
    directory = SimpleNamespace(
        db=database,
        profile_owned_wal=owned,
        _provider_directory_profile_capacity_admission=lambda: None,
        _PROFILE_CAPACITY_MUTATION_WINDOW=SimpleNamespace(get=lambda: None),
    )
    groups = []
    token = source._FACTORY.set(IssuedFactory(database, connection))
    try:
        with pytest.raises(asyncio.CancelledError) as caught:
            async with control.transaction(
                directory, None, identity=("capacity_admission",), enabled=True, groups=groups
            ):
                if body_fails:
                    raise primary
        group = groups[0]
        outcome = group["original_outcome"]
        assert isinstance(caught.value, owned.OwnedWalTransactionCancelled)
        assert caught.value.outcome is outcome is group["outcome"] and group["failure"] is caught.value
        if body_fails:
            body, cleanup = caught.value.__cause__.__cause__.exceptions
            assert body is primary and isinstance(cleanup, asyncio.CancelledError)
            assert connection.events.count("rollback") == 1
            assert outcome.commit_state == "not_attempted"
        else:
            assert caught.value.__cause__.exceptions == (cleanup_error,)
            assert outcome.commit_state == "confirmed"
        assert outcome.is_committed is not body_fails and outcome.cleanup_complete and outcome.measurement is None
        assert group["body_complete"] is not body_fails and not group["consumed"]
        assert not group["accounting_authority"] and not group["reservation_refund"]
        assert connection.closed and connection.driver.closed and database._transaction_binding() is None
    finally:
        source._FACTORY.reset(token)


async def test_source_preflight_reader_cleanup_cancel_preserves_body_and_capture(fixture, monkeypatch):
    database, connection = fixture
    primary = ValueError("synthetic reader body failure")
    cleanup_error = asyncio.CancelledError("synthetic reader rollback cancellation")

    async def rollback():
        connection.events.append("rollback")
        raise cleanup_error

    monkeypatch.setattr(connection, "rollback", rollback)
    monkeypatch.setattr(source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", lambda **_kwargs: nullcontext())
    monkeypatch.setattr(fhir.profile_scope_copy, "prepared_read_transaction", lambda _directory: nullcontext())
    capture_by_field = {}
    token = source._FACTORY.set(IssuedFactory(database, connection))
    try:
        with pytest.raises(asyncio.CancelledError) as caught:
            async with fhir._capture_profile_evidence_preflight_reader("synthetic", 1, capture_by_field):
                raise primary
        assert isinstance(caught.value, owned.OwnedWalTransactionCancelled)
        body, cleanup = caught.value.__cause__.__cause__.exceptions
        assert body is primary and isinstance(cleanup, asyncio.CancelledError)
        assert connection.events.count("rollback") == 1
        capture = capture_by_field[asyncio.current_task()]
        outcome = capture["native_outcome"]
        assert caught.value.outcome is outcome and outcome.commit_state == "not_attempted"
        assert not outcome.is_committed and outcome.cleanup_complete and outcome.measurement is None
        assert capture["failure_type"] == "OwnedWalTransactionCancelled" and capture["cleanup_complete"]
        assert capture["measurement_status"] == "incomplete" and not capture["committed"]
        assert not capture["accounting_authority"] and not capture["reservation_refund"]
        assert connection.closed and connection.driver.closed and database._transaction_binding() is None
    finally:
        source._FACTORY.reset(token)


async def test_unenrolled_source_refused_without_checkout():
    with pytest.raises(source.GenericCandidateError, match="transaction_factory_required"):
        async with source_owned_wal_transaction(SimpleNamespace(db=object())):
            pytest.fail("unenrolled Source accepted")


async def test_copy_cancellation_and_failed_custody_keep_cancelled_outcome(fixture):
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic COPY cleanup cancelled")
    with pytest.raises(owned.OwnedWalTransactionCancelled) as caught:
        async with owned.registry_retained_wal_transaction(connection) as outcome:
            async with owned.bind_retained_wal_transaction(database, outcome):
                connection.driver.closed = True
                connection.driver.in_transaction = False
                raise primary
    assert caught.value.outcome is outcome and outcome.cleanup_complete
    assert outcome.commit_state == "uncertain" and not outcome.is_committed and outcome.measurement is None
    failures = caught.value.__cause__.__cause__
    assert isinstance(failures, BaseExceptionGroup) and len(failures.exceptions) == 2
    assert failures.exceptions[0] is primary
    assert isinstance(failures.exceptions[1], RuntimeError)
    assert str(failures.exceptions[1]) == "provider_directory_owned_wal_custody_changed"
    assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    assert connection.closed and connection.driver.closed


@pytest.mark.parametrize("failure", ["cancel", "body", "return"])
async def test_closed_native_custody_stays_uncertain_after_unacknowledged_rollback(fixture, monkeypatch, failure):
    """A logical transaction cannot certify rollback after its native driver closes."""
    database, connection = fixture
    primary = asyncio.CancelledError("synthetic closed COPY") if failure == "cancel" else ValueError("synthetic body")
    rollback_error = OSError("synthetic closed native rollback")

    def transaction_status():
        if connection.driver.closed:
            raise AssertionError("closed native transaction state must not be queried")
        return connection.driver.in_transaction

    async def rollback():
        connection.events.append("rollback")
        connection.logical_transaction = None
        raise rollback_error

    monkeypatch.setattr(connection.driver, "is_in_transaction", transaction_status)
    monkeypatch.setattr(connection, "rollback", rollback)
    expected = owned.OwnedWalTransactionCancelled if failure == "cancel" else owned.OwnedWalTransactionError
    with pytest.raises(expected) as caught:
        async with owned.registry_retained_wal_transaction(connection) as outcome:
            async with owned.bind_retained_wal_transaction(database, outcome):
                connection.driver.closed = True
                connection.driver.in_transaction = False
                assert connection.in_transaction()
                if failure != "return":
                    raise primary
    assert caught.value.outcome is outcome and outcome.commit_state == "uncertain"
    assert not outcome.is_committed and outcome.measurement is None and outcome.cleanup_complete
    assert outcome.status == "commit_uncertain_accounting_incomplete"
    pending, causes = [caught.value], []
    while pending:
        error = pending.pop()
        causes.append(error)
        if error.__cause__ is not None:
            pending.append(error.__cause__)
        pending.extend(getattr(error, "exceptions", ()))
    if failure == "cancel":
        assert rollback_error in causes
    assert any(str(error) == "provider_directory_owned_wal_custody_changed" for error in causes)
    if failure != "return":
        assert primary in causes
    assert "commit" not in connection.events and connection.events.count("rollback") == 1
    assert connection.closed and connection.driver.closed and database._transaction_binding() is None
    assert owned.current_owned_wal_transaction(database) is None


@pytest.mark.parametrize("previous", [None, "repeatable_read"])
async def test_serializable_source_selects_before_begin_and_restores_after_cleanup(fixture, previous):
    database, connection = fixture
    connection.adapter.isolation_level = connection.adapter._isolation_setting = previous
    original = "READ COMMITTED" if previous is None else previous.replace("_", " ").upper()
    factory = IssuedFactory(database, connection)
    token = source._FACTORY.set(factory)
    try:
        async with source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable") as outcome:
            assert connection.isolation_level == "SERIALIZABLE"
            assert outcome.session is database._transaction_binding().session
            assert connection.events.index(("isolation", "SERIALIZABLE")) < connection.events.index("begin")
        assert outcome.is_committed and outcome.cleanup_complete and outcome.measurement is not None
        assert connection.events.index("commit") < connection.events.index(("isolation", original))
        assert (
            connection.isolation_level == original
            and connection.adapter.isolation_level == original.lower().replace(" ", "_")
        )
        assert factory.events[-1] == "guard_exit" and not connection.driver.is_in_transaction()
        assert connection.events.count("native_isolation") == (1 if previous is None else 0)
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("cancelled", [False, True])
async def test_isolation_restore_failure_keeps_commit_truth_and_retires_exact_backend(fixture, cancelled):
    database, connection = fixture
    connection.restore_failure = (
        asyncio.CancelledError("synthetic isolation restore cancellation")
        if cancelled
        else OSError("synthetic isolation restore failure")
    )
    factory = IssuedFactory(database, connection)
    token = source._FACTORY.set(factory)
    try:
        error_type = owned.OwnedWalTransactionCancelled if cancelled else owned.OwnedWalTransactionError
        with pytest.raises(error_type) as caught:
            async with source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable") as outcome:
                assert owned.current_owned_wal_transaction(database) is outcome
        assert caught.value.outcome is outcome and outcome.is_committed and outcome.cleanup_complete
        assert outcome.measurement is None and outcome.status == "committed_accounting_incomplete"
        assert caught.value.__cause__.exceptions == (connection.restore_failure,)
        assert connection.closed and connection.driver.closed
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("isolation", [None, True, "repeatable read", "SERIALIZABLE", "autocommit"])
async def test_unapproved_isolation_refused_before_factory_or_transport(isolation):
    with pytest.raises(source.GenericCandidateError, match="owned_wal_isolation_invalid"):
        async with source_owned_wal_transaction(SimpleNamespace(db=object()), isolation=isolation):
            pytest.fail("unapproved isolation accepted")


async def test_source_copy_cancelled_native_custody_preserves_both_failures(fixture):
    database, connection = fixture
    factory = IssuedFactory(database, connection)
    primary = asyncio.CancelledError("synthetic COPY termination")
    token = source._FACTORY.set(factory)
    try:
        with pytest.raises(owned.OwnedWalTransactionCancelled) as caught:
            async with source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable") as outcome:
                connection.driver.closed = True
                connection.driver.in_transaction = False
                raise primary
        assert caught.value.outcome is outcome and outcome.cleanup_complete and not outcome.is_committed
        failures = caught.value.__cause__.__cause__
        assert failures.exceptions[0] is primary
        assert str(failures.exceptions[1]) == "provider_directory_owned_wal_custody_changed"
        assert connection.closed and connection.driver.closed and outcome.measurement is None
        assert factory.events == ["guard_enter", "source_enter"]
    finally:
        source._FACTORY.reset(token)


async def test_prebegin_cancellation_and_restore_failure_remain_cancelled_without_outcome(fixture, monkeypatch):
    database, connection = fixture
    factory = IssuedFactory(database, connection)
    primary = asyncio.CancelledError("synthetic cancellation while selecting isolation")
    restore_error = OSError("synthetic pre-BEGIN isolation restore failure")
    changes = []

    async def change_isolation(*, isolation_level):
        assert not connection.in_transaction()
        changes.append(isolation_level)
        if len(changes) == 1:
            connection.isolation_level = isolation_level
            raise primary
        raise restore_error

    monkeypatch.setattr(connection, "execution_options", change_isolation)
    token = source._FACTORY.set(factory)
    try:
        with pytest.raises(asyncio.CancelledError) as caught:
            async with source_owned_wal_transaction(SimpleNamespace(db=database), isolation="serializable"):
                pytest.fail("cancelled isolation change reached its body")
        assert not isinstance(caught.value, owned.OwnedWalTransactionCancelled)
        assert isinstance(caught.value.__cause__, BaseExceptionGroup)
        assert caught.value.__cause__.exceptions == (primary, restore_error)
        assert changes == ["SERIALIZABLE", "READ COMMITTED"]
        assert "begin" not in connection.events and "commit" not in connection.events
        assert owned._RETAINED_OWNED_TRANSACTION.get() is None
        assert owned.current_owned_wal_transaction(database) is None and database._transaction_binding() is None
        assert connection.closed and connection.driver.closed
        assert factory.events == ["guard_enter"]
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("cancelled", [False, True])
async def test_control_group_preserves_plain_prebegin_failure_and_cleanup_cause(fixture, monkeypatch, cancelled):
    database, connection = fixture
    factory = IssuedFactory(database, connection)
    primary = (
        asyncio.CancelledError("synthetic isolation cancellation") if cancelled else ValueError("synthetic origin")
    )
    cleanup_error = OSError("synthetic isolation cleanup failure")
    changes = []

    async def change_isolation(*, isolation_level):
        changes.append(isolation_level)
        if len(changes) == 1:
            connection.isolation_level = isolation_level
            raise primary
        raise cleanup_error

    async def failed_origin(statement, *parameters):
        assert statement == "SHOW transaction_isolation" and not parameters
        raise primary from cleanup_error

    if cancelled:
        monkeypatch.setattr(connection, "execution_options", change_isolation)
    else:
        monkeypatch.setattr(connection.driver, "fetchval", failed_origin)
    monkeypatch.setattr(control, "is_source_storage_required", lambda: True)
    directory = SimpleNamespace(
        db=database,
        profile_owned_wal=owned,
        _provider_directory_profile_capacity_admission=lambda: None,
        _PROFILE_CAPACITY_MUTATION_WINDOW=SimpleNamespace(get=lambda: None),
    )
    groups = []
    token = source._FACTORY.set(factory)
    try:
        with pytest.raises(asyncio.CancelledError if cancelled else ValueError) as caught:
            async with control.transaction(
                directory, None, identity=("capacity_admission",), enabled=True, groups=groups
            ):
                pytest.fail("failed isolation reached control body")
        group = groups[0]
        assert group["failure"] is caught.value and group["outcome"] is group["original_outcome"] is None
        assert not group["consumed"] and not group["body_complete"] and group["status"] == "incomplete"
        assert not group["accounting_authority"] and not group["reservation_refund"]
        if cancelled:
            assert not isinstance(caught.value, owned.OwnedWalTransactionCancelled)
            assert caught.value.__cause__.exceptions == (primary, cleanup_error)
            assert connection.closed and connection.driver.closed
            assert changes == ["SERIALIZABLE", "READ COMMITTED"]
        else:
            assert caught.value is primary and caught.value.__cause__ is cleanup_error
        assert "begin" not in connection.events and "commit" not in connection.events
        assert owned.current_owned_wal_transaction(database) is None and database._transaction_binding() is None
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("isolation", ["read committed", "repeatable read", "serializable"])
async def test_isolation_uses_original_adapter_mode(fixture, monkeypatch, cached, isolation):
    _database, connection = fixture
    if cached:
        PGDialect_asyncpg().set_isolation_level(connection.adapter, isolation.upper())
    origin = await source_wal._isolation_origin(connection)
    native_read = AsyncMock(return_value="read committed" if cached else isolation)
    monkeypatch.setattr(connection.driver, "fetchval", native_read)
    assert await source_wal._read_isolation(connection, origin) == isolation.upper()
    if cached:
        native_read.assert_not_awaited()
    else:
        native_read.assert_awaited_once_with("SHOW transaction_isolation")
    assert not connection.driver.is_in_transaction() and connection.driver.get_server_pid() == origin[3]
    assert (await connection.get_raw_connection()).dbapi_connection is origin[4]


@pytest.mark.parametrize(
    "cached,isolation",
    [(True, mode) for mode in (True, 0, "autocommit", "read_uncommitted", "READ COMMITTED", "read committed")]
    + [(False, mode) for mode in (None, True, 0, "autocommit", "read uncommitted", "READ COMMITTED", "read_committed")],
)
async def test_isolation_rejects_unsupported_modes(fixture, monkeypatch, cached, isolation):
    _database, connection = fixture
    connection.adapter.isolation_level = isolation if cached else None
    monkeypatch.setattr(connection.driver, "fetchval", AsyncMock(return_value=isolation))
    setting = AsyncMock(side_effect=AssertionError("unsupported mode reached execution_options"))
    monkeypatch.setattr(connection, "execution_options", setting)
    with pytest.raises(RuntimeError, match="source_wal_isolation_changed"):
        async with source_wal._source_isolation(connection, "SERIALIZABLE", [None]):
            pytest.fail("unsupported isolation reached the transaction body")
    setting.assert_not_awaited()
    assert "begin" not in connection.events


@pytest.mark.parametrize("fault", ["transaction", "pid", "adapter", "cache"])
async def test_isolation_rechecks_original_backend(fixture, monkeypatch, fault):
    _database, connection = fixture
    origin = await source_wal._isolation_origin(connection)
    driver, adapter, original_pid = connection.driver, connection.adapter, connection.driver.pid

    async def changed_read(statement):
        assert statement == "SHOW transaction_isolation"
        if fault == "transaction":
            driver.in_transaction = True
        elif fault == "pid":
            driver.pid += 1
        elif fault == "adapter":
            connection.adapter = AsyncAdapt_asyncpg_connection(None, driver)
        else:
            adapter.isolation_level = "serializable"
        return "read committed"

    monkeypatch.setattr(driver, "fetchval", changed_read)
    try:
        with pytest.raises(RuntimeError, match="isolation_changed" if fault == "cache" else "custody_changed"):
            await source_wal._read_isolation(connection, origin)
    finally:
        driver.in_transaction = False
        driver.pid = original_pid
        connection.adapter = adapter
