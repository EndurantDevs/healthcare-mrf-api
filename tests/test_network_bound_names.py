# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-name read inventories prove binding identity, without claiming data consumption."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import network_read_execution as execution
from db.connection import ConnectionProxy, Database, SelectAdapter
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError, verify_bound_network_names
from tests.test_network_read_execution import _session

_PIN = CompactAddressRelation("held_snapshot", "provider_profile", 20)


@pytest.mark.parametrize("damage", [None, "pid", "count", "name", "transaction", "missing"])
async def test_names_receipt_requires_the_original_backend_and_complete_mapping(damage):
    receipt_by_field = {"backend_matches": True, "relation_count": 1, "valid": True}
    if damage in {"pid", "name"}:
        receipt_by_field["backend_matches" if damage == "pid" else "valid"] = False
    if damage == "count":
        receipt_by_field["relation_count"] = True
    connection = SimpleNamespace(
        is_in_transaction=Mock(side_effect=[True, damage != "transaction"]),
        fetchrow=AsyncMock(return_value=None if damage == "missing" else receipt_by_field),
    )
    operation = verify_bound_network_names(connection, (_PIN,), expected_backend_pid=42)
    if damage:
        with pytest.raises(NetworkReadIdentityError):
            await operation
    else:
        await operation
    query, names, oids, pid = connection.fetchrow.await_args.args
    assert names == ['"held_snapshot"."provider_profile"'] and oids == [20] and pid == 42
    assert "LOCK TABLE" not in query and "pg_advisory" not in query and "pg_locks" not in query


async def test_empty_inventory_touches_no_connection_and_establishes_no_authority():
    await verify_bound_network_names(object(), (), expected_backend_pid=42)


async def test_absence_is_an_explicit_checked_binding():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"backend_matches": True, "relation_count": 1, "valid": True}),
    )
    await verify_bound_network_names(
        connection, (), expected_backend_pid=42, absent_names=(("held_snapshot", "missing_profile"),)
    )
    _, names, oids, pid = connection.fetchrow.await_args.args
    assert names == ['"held_snapshot"."missing_profile"'] and oids == [None] and pid == 42


@pytest.mark.parametrize("absent", [[("held_snapshot", "heap")], (("bad.name", "heap"),), (("one",),)])
async def test_invalid_absence_refuses_before_query(absent):
    with pytest.raises(NetworkReadIdentityError):
        await verify_bound_network_names(object(), (), expected_backend_pid=42, absent_names=absent)


async def test_installed_and_absent_coordinates_cannot_conflict():
    with pytest.raises(NetworkReadIdentityError):
        await verify_bound_network_names(
            object(), (_PIN,), expected_backend_pid=42, absent_names=((_PIN.schema_name, _PIN.table_name),)
        )


@pytest.mark.parametrize(
    "pins",
    [(_PIN,) * 4097, [_PIN], (SimpleNamespace(),), (_PIN, CompactAddressRelation("other", "heap", 20))],
)
async def test_invalid_inventory_refuses_before_native_query(pins):
    with pytest.raises(NetworkReadIdentityError):
        await verify_bound_network_names(object(), pins, expected_backend_pid=42)


@pytest.mark.parametrize("refuse", [False, True])
async def test_fixed_inventory_guards_unmarked_buffered_reads_before_cache(monkeypatch, refuse):
    events, cache = [], []
    session, driver, result = _session(events)

    async def verify(actual, pins, *, expected_backend_pid):
        assert actual is driver and pins == (_PIN,) and expected_backend_pid == 42
        assert events == ["execute"]
        events.append("names")
        if refuse:
            raise NetworkReadIdentityError("name changed")

    monkeypatch.setattr(execution, "verify_bound_network_names", verify)
    with execution.network_read_name_inventory(driver, (_PIN,)):
        statement = text('SELECT * FROM "held_snapshot"."provider_profile"')
        assert execution.has_network_read_guard(statement)
        operation = execution.execute_network_read(session, statement)
        if refuse:
            with pytest.raises(ServiceUnavailable):
                await operation
            result.close.assert_called_once()
        else:
            cache.extend((await operation).all())
    assert events == ["execute", "names"] and cache == ([] if refuse else [("office",)])
    assert not execution.has_network_read_guard(statement)


async def test_inventory_cannot_move_to_another_driver_before_data_sql():
    session, _, _ = _session([])
    with execution.network_read_name_inventory(SimpleNamespace(get_server_pid=lambda: 42), (_PIN,)):
        with pytest.raises(ServiceUnavailable):
            await execution.execute_network_read(session, text("SELECT 1"))
    session.execute.assert_not_awaited()


def test_mutated_coordinates_and_closed_driver_use_private_api_refusal():
    mutated = CompactAddressRelation("held_snapshot", "provider_profile", 20)
    object.__setattr__(mutated, "table_oid", True)
    with pytest.raises(ServiceUnavailable):
        execution.NetworkReadStatements("scope").marker((mutated,))
    with (
        pytest.raises(ServiceUnavailable),
        execution.network_read_name_inventory(SimpleNamespace(get_server_pid=lambda: 42), (mutated,)),
    ):
        raise AssertionError("invalid inventory was entered")
    closed = SimpleNamespace(get_server_pid=Mock(side_effect=RuntimeError("closed")))
    with pytest.raises(ServiceUnavailable), execution.network_read_name_inventory(closed, (_PIN,)):
        raise AssertionError("closed inventory was entered")


async def test_native_driver_acquisition_failure_precedes_payload_sql():
    session, driver, _ = _session([])
    with execution.network_read_name_inventory(driver, (_PIN,)):
        session.connection.side_effect = RuntimeError("closed")
        with pytest.raises(ServiceUnavailable):
            await execution.execute_network_read(session, text("SELECT 1"))
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("adapter", ["database", "statement", "proxy"])
@pytest.mark.parametrize("method", ["all", "first", "scalar"])
@pytest.mark.parametrize("refuse", [False, True])
async def test_db_buffered_seams_guard_before_return(monkeypatch, adapter, method, refuse):
    events, cache = [], []
    session, driver, buffered = _session(events)
    buffered.first, buffered.scalar = lambda: "office", lambda: 20
    database = Database()

    @asynccontextmanager
    async def original_session():
        yield session

    monkeypatch.setattr(database, "_execution_session", original_session)

    async def verify(actual, pins, *, expected_backend_pid):
        assert actual is driver and pins == (_PIN,) and expected_backend_pid == 42
        assert events == ["execute"]
        events.append("names")
        if refuse:
            raise NetworkReadIdentityError("name changed")

    monkeypatch.setattr(execution, "verify_bound_network_names", verify)
    statement = text('SELECT * FROM "held_snapshot"."provider_profile"')
    read_adapter = {
        "database": database,
        "statement": SelectAdapter(database, statement),
        "proxy": ConnectionProxy(database, session, None),
    }[adapter]
    arguments = () if adapter == "statement" else (statement,)
    with execution.network_read_name_inventory(driver, (_PIN,)):
        operation = getattr(read_adapter, method)(*arguments)
        if refuse:
            with pytest.raises(ServiceUnavailable):
                cache.append(await operation)
            buffered.close.assert_called_once()
        else:
            cache.append(await operation)
    assert events == ["execute", "names"]
    assert cache == ([] if refuse else [{"all": [("office",)], "first": "office", "scalar": 20}[method]])


def _installable_session():
    session, driver, buffered = _session([])
    transaction = object()
    session.info = {}
    session.get_transaction = lambda: transaction
    driver.is_in_transaction = lambda: True
    driver.fetchrow = AsyncMock(return_value={"backend_matches": True, "relation_count": 1, "valid": True})
    return session, driver, buffered


async def test_installed_inventory_checks_unmarked_data_and_expires_with_transaction():
    session, driver, buffered = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,))
    assert execution.has_network_read_inventory(session) and driver.fetchrow.await_count == 1
    assert await execution.execute_network_read(session, text("SELECT 1")) is buffered
    assert driver.fetchrow.await_count == 2
    session.get_transaction = lambda: object()
    with pytest.raises(ServiceUnavailable):
        await execution.execute_network_read(session, text("SELECT 1"))
    assert session.execute.await_count == 1


async def test_failed_install_never_exposes_a_session_inventory():
    session, driver, _ = _installable_session()
    driver.fetchrow.return_value["valid"] = False
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, (_PIN,))
    assert not execution.has_network_read_inventory(session)


async def test_installed_inventory_is_checked_before_buffered_rows_escape():
    session, driver, buffered = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,))
    driver.fetchrow.return_value["valid"] = False
    with pytest.raises(ServiceUnavailable):
        await execution.execute_network_read(session, text("SELECT 1"))
    buffered.close.assert_called_once()


async def test_second_install_cannot_replace_a_pinned_name_with_another_oid():
    session, driver, _ = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,))
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(
            session, (CompactAddressRelation(_PIN.schema_name, _PIN.table_name, _PIN.table_oid + 1),)
        )
    assert driver.fetchrow.await_count == 1


@pytest.mark.parametrize("mutation", ["transaction", "driver", "pid", "inactive"])
async def test_inventory_install_rechecks_context_after_awaited_guard(mutation):
    session, driver, _ = _installable_session()

    async def rotated(*_arguments):
        if mutation == "transaction":
            session.get_transaction = lambda: object()
        elif mutation == "driver":
            other = SimpleNamespace(get_server_pid=lambda: 42)
            connection = SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=other))
            )
            session.connection = AsyncMock(return_value=connection)
        elif mutation == "pid":
            driver.get_server_pid = lambda: 43
        else:
            driver.is_in_transaction = lambda: False
        return {"backend_matches": True, "relation_count": 1, "valid": True}

    driver.fetchrow.side_effect = rotated
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, (_PIN,))
    assert not execution.has_network_read_inventory(session)


async def test_inventory_merge_does_not_accept_a_mutable_pin_container():
    session, driver, _ = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,))
    installed = session.info["api_network_read_inventory"]
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, [_PIN])
    assert session.info["api_network_read_inventory"] is installed and driver.fetchrow.await_count == 1


@pytest.mark.parametrize("damage", ["mutable", "uninstalled", "wrong-oid"])
async def test_direct_heap_pins_require_an_exact_typed_inventory_subset(damage):
    session, driver, _ = _installable_session()
    only = {
        "mutable": [_PIN],
        "uninstalled": (CompactAddressRelation("other", "office", 99),),
        "wrong-oid": (CompactAddressRelation(_PIN.schema_name, _PIN.table_name, _PIN.table_oid + 1),),
    }[damage]
    with pytest.raises(ServiceUnavailable):
        await execution.install_network_read_inventory(session, (_PIN,), only_relations=only)
    driver.fetchrow.assert_not_awaited()
    assert not execution.has_network_read_inventory(session)


async def test_direct_heap_pins_are_retained_when_later_inventories_are_merged():
    session, driver, _ = _installable_session()
    driver.fetchrow.side_effect = lambda _query, names, _oids, _pid: {
        "backend_matches": True,
        "relation_count": len(names),
        "valid": True,
    }
    await execution.install_network_read_inventory(session, (_PIN,), only_relations=(_PIN,))
    extra = CompactAddressRelation("other", "office", 99)
    await execution.install_network_read_inventory(session, (extra,))
    installed = execution._installed_read_inventory(session)
    assert installed.only_relations == (_PIN,) and set(installed.relations) == {_PIN, extra}


@pytest.mark.parametrize("direct", [False, True])
async def test_fixed_sql_only_uses_the_exact_independently_proved_name(direct):
    session, _driver, _result = _installable_session()
    qualified = '"held_snapshot"."provider_profile"'
    assert execution.direct_heap_relation_sql(session, _PIN.schema_name, _PIN.table_name) == qualified
    await execution.install_network_read_inventory(session, (_PIN,), only_relations=(_PIN,) if direct else ())
    assert execution.direct_heap_relation_sql(session, _PIN.schema_name, _PIN.table_name) == (
        "ONLY " + qualified if direct else qualified
    )
    assert execution.direct_heap_relation_sql(session, "another_schema", _PIN.table_name) == (
        '"another_schema"."provider_profile"'
    )
    assert execution.direct_heap_relation_sql(session, _PIN.schema_name, "another_table") == (
        '"held_snapshot"."another_table"'
    )


@pytest.mark.parametrize("name", [None, "bad.name", 'table"; SELECT 1', "heap alias", 1])
def test_fixed_sql_relation_rejects_fragments_without_touching_a_session(name):
    with pytest.raises(ValueError, match="identifier"):
        execution.direct_heap_relation_sql(object(), "safe_schema", name)


async def test_fixed_sql_cannot_render_under_a_replaced_transaction():
    session, _driver, _result = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,), only_relations=(_PIN,))
    session.get_transaction = lambda: object()
    with pytest.raises(ServiceUnavailable):
        execution.direct_heap_relation_sql(session, _PIN.schema_name, _PIN.table_name)
