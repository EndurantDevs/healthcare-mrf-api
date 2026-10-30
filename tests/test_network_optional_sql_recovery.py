# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guard/recovery protocol only; these native doubles establish no Source authority."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InterfaceError

from api import network_read_execution as execution
from api import provider_profile_snapshot as snapshot
from api import ptg2_serving as serving
from api.network_address_scope import network_address_read_scope
from process import network_read_identity as identity
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_read_execution import _manifest

_PIN = CompactAddressRelation("held", "parent", 23)
_ABSENT = (("held", "optional"),)


class _Nested:
    nested = True

    def __init__(self, session):
        self.session, self.parent = session, session.nested
        self.is_active, self.exited = True, False

    async def __aenter__(self):
        self.session.events.append("enter")
        self.session.nested = self
        return self

    async def rollback(self):
        self.session.events.append("rollback")
        if self.session.rollback_error is not None:
            raise self.session.rollback_error
        self.is_active = self.session.aborted = False
        self.session.nested = self.parent
        self.session.last_rollback = self
        if self.session.after_rollback:
            self.session.after_rollback()

    async def __aexit__(self, kind, value, traceback):
        if self.is_active and kind is not None:
            await self.rollback()
        self.is_active, self.exited = False, True
        self.session.nested = self.parent
        self.session.events.append("exit")
        if self.session.exit_error is not None:
            raise self.session.exit_error


class _Driver:
    def __init__(self, session):
        self.session, self.pid, self.active = session, 42, True

    def get_server_pid(self):
        return self.pid

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, sql, *parameters):
        session = self.session
        if sql in (identity._BOUND_NAMES_SQL, identity._CONSUMED_SQL):
            assert not session.aborted
            if session.last_rollback is not None:
                assert session.last_rollback.exited, "recovery queried inside a closed native context"
            names, oids, pid, *_ = parameters
            session.events.append("consumed" if sql == identity._CONSUMED_SQL else "names")
            receipt_by_field = {
                "backend_matches": self.pid == pid,
                "relation_count": len(names),
                "valid": all(session.names.get(name) == oid for name, oid in zip(names, oids, strict=True)),
            }
            if session.after_guard:
                session.after_guard()
            return receipt_by_field
        session.events.append("native query")
        if session.error is not None:
            error, session.error, session.aborted = session.error, None, True
            raise error
        if session.after_query:
            session.after_query()
        return {"count": 0, "bytes": 0, "rows_json": None}


class _Session:
    def __init__(self, error=None):
        self.info, self.transaction, self.nested = {}, object(), None
        self.events, self.names = [], {'"held"."parent"': 23}
        self.executions = []
        self.error, self.aborted, self.last_rollback = error, False, None
        self.rollback_error = self.exit_error = self.after_rollback = self.after_query = None
        self.after_guard = None
        self.driver = _Driver(self)
        self.root_rollback, self.root_commit = Mock(), Mock()
        self.result = SimpleNamespace(all=Mock(return_value=[1]), close=Mock())

    def get_transaction(self):
        return self.transaction

    def get_nested_transaction(self):
        return self.nested

    def in_transaction(self):
        return True

    def begin_nested(self):
        return _Nested(self)

    async def connection(self):
        async def raw():
            return SimpleNamespace(driver_connection=self.driver)

        return SimpleNamespace(get_raw_connection=raw)

    async def execute(self, statement, parameters):
        self.events.append("query")
        self.executions.append((statement, parameters))
        if self.error is not None:
            error, self.error, self.aborted = self.error, None, True
            raise error
        if self.after_query:
            self.after_query()
        return self.result


async def _installed(error=None):
    session = _Session(error)
    await execution.install_network_read_inventory(session, (_PIN,), absent_names=_ABSENT, only_relations=(_PIN,))
    session.events.clear()
    return session


async def _query(session, adapter="buffered"):
    if adapter == "native":
        return await execution.bounded_network_read_rows(session, 'SELECT id FROM ONLY "held"."parent"')
    return await execution.execute_network_read(session, text('SELECT id FROM ONLY "held"."parent"'))


@pytest.mark.parametrize("adapter", ["buffered", "native"])
@pytest.mark.parametrize(
    "error",
    [asyncpg.UndefinedTableError("missing"), DBAPIError("private SQL", {}, asyncpg.UndefinedTableError("missing"))],
)
async def test_actual_guarded_sql_failures_are_private_503_with_original_cause(adapter, error):
    session = await _installed(error)
    with pytest.raises(ServiceUnavailable) as caught:
        await _query(session, adapter)
    assert type(caught.value) is execution._ReadSQLFailure and caught.value.__cause__ is error
    assert caught.value.headers["Cache-Control"] == "private, no-store"
    assert "private SQL" not in str(caught.value)
    assert session.events == ["native query" if adapter == "native" else "query"]


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.DivisionByZeroError("division"),
        asyncpg.QueryCanceledError("timeout"),
        DBAPIError(None, None, asyncpg.DivisionByZeroError("division")),
    ],
)
@pytest.mark.parametrize("adapter", ["buffered", "native"])
async def test_owned_rollback_then_name_revalidation_restores_original_error_and_later_read(error, adapter):
    session = await _installed(error)
    original_transaction, original_driver, original_cause = session.transaction, session.driver, error.__cause__
    with pytest.raises(type(error)) as caught:
        async with snapshot.provider_read_savepoint(session):
            await _query(session, adapter)
    assert caught.value is error and error.__cause__ is original_cause
    assert session.events[:4] == ["enter", "native query" if adapter == "native" else "query", "rollback", "exit"]
    assert session.events[4:] == ["names"] and "consumed" not in session.events
    assert session.transaction is original_transaction and session.driver is original_driver and session.driver.active
    assert (await _query(session)).all() == [1]
    session.root_rollback.assert_not_called()
    session.root_commit.assert_not_called()


@pytest.mark.parametrize("mode", ["inventory", "marker"])
async def test_context_and_marker_names_are_revalidated_without_consumed_lock_proof(mode):
    session = _Session(asyncpg.DivisionByZeroError("division"))
    scope = (
        network_address_read_scope(_manifest(), (42,))
        if mode == "marker"
        else execution.network_read_name_inventory(session.driver, (_PIN,), absent_names=_ABSENT)
    )
    with scope as active:
        marker = active.read_statements.marker((_PIN,)) if mode == "marker" else ""
        logger = SimpleNamespace(debug=Mock())
        async with snapshot.provider_optional_read(session, logger):
            await execution.execute_network_read(session, text(f"SELECT {marker} 1"))
        assert session.events == ["enter", "query", "rollback", "exit", "names"]
        logger.debug.assert_called_once()


@pytest.mark.parametrize(
    "damage", ["oid", "rename", "absence", "root", "driver", "pid", "inactive", "installed", "context"]
)
async def test_recovery_never_rebaselines_changed_identity_or_name_inventory(damage):
    session = await _installed(asyncpg.DivisionByZeroError("division"))
    token_by_kind = {"active": None}

    def change():
        if damage == "context":
            token_by_kind["active"] = execution._NAME_INVENTORIES.set(
                (execution._NameInventory(session.driver, (_PIN,), 42),)
            )
            return
        if damage == "installed":
            old = session.info[execution._SESSION_INVENTORY_KEY]
            session.info[execution._SESSION_INVENTORY_KEY] = execution._SessionInventory(old.inventory, old.transaction)
            return
        changes_by_kind = {
            "oid": lambda: session.names.update({'"held"."parent"': 24}),
            "rename": lambda: session.names.pop('"held"."parent"'),
            "absence": lambda: session.names.update({'"held"."optional"': 25}),
            "root": lambda: setattr(session, "transaction", object()),
            "driver": lambda: setattr(session, "driver", _Driver(session)),
            "pid": lambda: setattr(session.driver, "pid", 43),
            "inactive": lambda: setattr(session.driver, "active", False),
        }
        changes_by_kind[damage]()

    session.after_rollback = change
    try:
        with pytest.raises(ServiceUnavailable) as caught:
            async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
                await _query(session)
        assert caught.value.headers["Cache-Control"] == "private, no-store"
        assert session.events.count("query") == 1
    finally:
        if token_by_kind["active"] is not None:
            execution._NAME_INVENTORIES.reset(token_by_kind["active"])


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.InterfaceError("interface"),
        asyncpg.ConnectionDoesNotExistError("disconnect"),
        InterfaceError(None, None, asyncpg.InterfaceError("interface")),
        DBAPIError(None, None, asyncpg.InterfaceError("wrapped interface")),
        DBAPIError(None, None, asyncpg.DivisionByZeroError("invalidated"), connection_invalidated=True),
        RuntimeError("not a database statement error"),
        ServiceUnavailable("an identity refusal"),
    ],
)
async def test_nonrecoverable_query_errors_cannot_become_optional_fallback(error):
    if isinstance(error, RuntimeError):
        error.sqlstate = "57014"
    if isinstance(error, DBAPIError) and isinstance(error.orig, asyncpg.InterfaceError):
        error.orig.sqlstate = "57014"
    session = await _installed(error)
    with pytest.raises(ServiceUnavailable):
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert session.events.count("query") == 1 and "names" not in session.events


@pytest.mark.parametrize("failure", ["rollback", "context exit"])
async def test_cleanup_failure_never_authorizes_legacy_fallback(failure):
    session = await _installed(asyncpg.DivisionByZeroError("division"))
    setattr(session, "rollback_error" if failure == "rollback" else "exit_error", RuntimeError("cleanup failed"))
    with pytest.raises(ServiceUnavailable) as caught:
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert caught.value.headers["Cache-Control"] == "private, no-store" and "names" not in session.events


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_python_cancellation_remains_the_same_object_after_cleanup(cleanup_fails):
    cancellation = asyncio.CancelledError("cancelled")
    session = await _installed(cancellation)
    if cleanup_fails:
        session.rollback_error = RuntimeError("cleanup failed")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert caught.value is cancellation and "names" not in session.events


async def test_successful_query_guard_refusal_cannot_recover_even_when_rollback_restores_name():
    session = await _installed()
    session.after_query = lambda: session.names.update({'"held"."parent"': 24})
    session.after_rollback = lambda: session.names.update({'"held"."parent"': 23})
    with pytest.raises(ServiceUnavailable) as caught:
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert type(caught.value) is not execution._ReadSQLFailure
    session.result.close.assert_called_once()
    session.result.all.assert_not_called()


@pytest.mark.parametrize("consumer", ["columns", "directory", "procedure"])
async def test_ptg_optional_callers_propagate_unrecovered_identity_failure(consumer):
    session = await _installed(asyncpg.UndefinedTableError("gone"))
    session.after_rollback = lambda: session.names.pop('"held"."parent"')
    with pytest.raises(ServiceUnavailable):
        if consumer == "columns":
            await serving._ptg2_table_columns(session, "mrf.npi")
        elif consumer == "directory":
            await serving._provider_directory_corroboration_by_key(
                session,
                "mrf.directory",
                [(1000000001, "synthetic")],
                plan_id=None,
                snapshot_id=None,
                source_key=None,
            )
        else:
            await serving._procedure_details_for_rows(
                session, [{"reported_code_system": "CPT", "reported_code": "00100"}]
            )


async def test_unguarded_query_errors_and_optional_fallback_remain_legacy():
    error = RuntimeError("optional")
    session = _Session(error)
    with pytest.raises(RuntimeError) as caught:
        await _query(session)
    assert caught.value is error
    session.error, session.aborted = RuntimeError("optional"), False
    logger = SimpleNamespace(debug=Mock())
    async with snapshot.provider_optional_read(session, logger):
        await _query(session)
    logger.debug.assert_called_once()


@pytest.mark.parametrize("damage", ["name", "marker registry"])
async def test_marker_only_recovery_rejects_changed_marker_names_and_context(damage):
    session = _Session(asyncpg.DivisionByZeroError("division"))
    with network_address_read_scope(_manifest(), (42,)) as scope:
        marker = scope.read_statements.marker((_PIN,))

        def change():
            if damage == "name":
                session.names['"held"."parent"'] = 24
            else:
                scope.read_statements.relations_by_marker.clear()

        session.after_rollback = change
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
                await execution.execute_network_read(session, text(f"SELECT {marker} 1"))
        assert "consumed" not in session.events


async def test_a_handmade_rollback_receipt_is_not_recovery_permission():
    session = await _installed(asyncpg.DivisionByZeroError("division"))
    with pytest.raises(execution._ReadSQLFailure) as caught:
        await _query(session)
    forged = execution._OptionalReadRollback(caught.value, SimpleNamespace(is_active=False), None, object())
    with pytest.raises(ServiceUnavailable):
        await execution._recover_optional_read(session, forged)
    assert "names" not in session.events


@pytest.mark.parametrize("adapter", ["buffered", "native"])
@pytest.mark.parametrize("damage", ["installed", "context", "marker"])
@pytest.mark.parametrize("timing", ["query", "guard"])
async def test_successful_query_cannot_escape_into_changed_active_scope(adapter, damage, timing):
    session = await _installed()
    token_by_kind = {"active": None}
    with network_address_read_scope(_manifest(), (42,)) as scope:
        marker = scope.read_statements.marker((_PIN,))

        def change():
            session.after_query = session.after_guard = None
            if damage == "installed":
                old = session.info[execution._SESSION_INVENTORY_KEY]
                session.info[execution._SESSION_INVENTORY_KEY] = execution._SessionInventory(
                    old.inventory, old.transaction
                )
            elif damage == "context":
                token_by_kind["active"] = execution._NAME_INVENTORIES.set(
                    (execution._NameInventory(session.driver, (_PIN,), 42),)
                )
            else:
                scope.read_statements.relations_by_marker.clear()

        setattr(session, "after_query" if timing == "query" else "after_guard", change)
        try:
            with pytest.raises(ServiceUnavailable):
                if adapter == "native":
                    await execution.bounded_network_read_rows(session, f"SELECT {marker} id FROM held.parent")
                else:
                    await execution.execute_network_read(session, text(f"SELECT {marker} id FROM held.parent"))
            if adapter == "buffered":
                session.result.close.assert_called_once()
                session.result.all.assert_not_called()
        finally:
            if token_by_kind["active"] is not None:
                execution._NAME_INVENTORIES.reset(token_by_kind["active"])


async def test_unused_marker_registration_and_sequential_inventory_merge_remain_allowed():
    session = await _installed()
    with network_address_read_scope(_manifest(), (42,)) as scope:
        marker = scope.read_statements.marker((_PIN,))
        session.after_query = lambda: scope.read_statements.marker((CompactAddressRelation("held", "unused", 33),))
        assert await execution.execute_network_read(session, text(f"SELECT {marker} 1")) is session.result
    session.after_query = None
    extra = CompactAddressRelation("held", "next", 24)
    session.names['"held"."next"'] = 24
    await execution.install_network_read_inventory(session, (extra,), only_relations=(extra,))
    assert await _query(session) is session.result


@pytest.mark.parametrize("installed", [False, True])
async def test_procedure_catalog_only_slot_preserves_original_default_and_bound_columns(monkeypatch, installed):
    from tests.test_pricing_api import FakeResult

    session = _Session()
    session.result = FakeResult(
        rows=[
            {
                "code_system": "CPT",
                "code": "00100",
                "display_name": "Synthetic",
                "short_description": "Detail",
            }
        ]
    )
    session.result.close = Mock()
    monkeypatch.setattr(serving, "PTG2_SCHEMA", "mrf")
    if installed:
        pin = CompactAddressRelation("mrf", "code_catalog", 31)
        session.names['"mrf"."code_catalog"'] = 31
        await execution.install_network_read_inventory(session, (pin,), only_relations=(pin,))
    assert await serving._procedure_details_for_rows(
        session, [{"reported_code_system": "CPT", "reported_code": "00100"}]
    ) == {("CPT", "00100"): {"procedure_name": "Synthetic", "procedure_description": "Detail"}}
    statement, parameters = session.executions[-1]
    assert ('FROM ONLY "mrf"."code_catalog"' if installed else "FROM mrf.code_catalog") in str(statement)
    assert parameters == {"code_system_0": "CPT", "code_0": "00100"}


async def test_failed_recovery_receipt_cannot_be_retried_after_names_are_restored(monkeypatch):
    session = await _installed(asyncpg.DivisionByZeroError("division"))
    session.after_rollback = lambda: session.names.update({'"held"."parent"': 24})
    issued_rollbacks = []

    async def recover(actual_session, rollback):
        issued_rollbacks.append(rollback)
        return await execution._recover_optional_read(actual_session, rollback)

    monkeypatch.setattr(snapshot, "_recover_optional_read", recover)
    with pytest.raises(ServiceUnavailable):
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    calls = session.events.count("names")
    session.names['"held"."parent"'] = 23
    with pytest.raises(ServiceUnavailable):
        await execution._recover_optional_read(session, issued_rollbacks[0])
    assert session.events.count("names") == calls and session.events.count("query") == 1
