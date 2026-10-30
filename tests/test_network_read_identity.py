# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed-relation guards stay on the caller's backend and precede exposure."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import (
    MAX_CONSUMED_RELATIONS,
    NetworkReadIdentityError,
    verify_consumed_network_relations,
)

pytestmark = pytest.mark.asyncio
_PID = 42


def _connection(**changes):
    return SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value={"backend_matches": True, "relation_count": 1, "valid": True} | changes),
        acquire=Mock(side_effect=AssertionError("another backend cannot verify this read")),
    )


def _relation():
    return CompactAddressRelation("PinnedSchema", "network_office_edge", 4294967295)


async def test_empty_consumption_does_not_touch_connection():
    class NoConnection:
        def __getattr__(self, name):
            raise AssertionError("empty consumption touched a connection: " + name)

    assert await verify_consumed_network_relations(NoConnection(), (), expected_backend_pid=_PID) is None


async def test_native_guard_uses_one_bound_statement():
    connection = _connection(relation_count=2)
    first = _relation()
    second = replace(first, table_name="network_slice_ref", table_oid=100)
    assert (
        await verify_consumed_network_relations(connection, (first, second, first), expected_backend_pid=_PID) is None
    )
    connection.acquire.assert_not_called()
    connection.fetchrow.assert_awaited_once()
    query, names, oids, pid, modes = connection.fetchrow.await_args.args
    assert names == ['"PinnedSchema"."network_office_edge"', '"PinnedSchema"."network_slice_ref"']
    assert oids == [4294967295, 100] and pid == _PID
    assert all(name not in query for name in names)
    assert "pg_backend_pid()=$3::integer" in query and "pg_catalog.to_regclass(expected.relation_name)" in query
    assert "database=(" in query and "locktype='relation'" in query and "AND granted" in query
    assert "FROM pg_catalog.pg_database WHERE datname=current_database()" in query
    assert "AccessShareLock" in modes and "SIReadLock" not in modes
    assert all(token not in query for token in ("LOCK TABLE", "pg_advisory", "LIMIT 0", "SET ROLE"))
    assert connection.is_in_transaction.call_count == 2


async def test_complete_batch_uses_one_query():
    relations = tuple(
        CompactAddressRelation("pinned", "table_" + str(index), index + 1) for index in range(MAX_CONSUMED_RELATIONS)
    )
    connection = _connection(relation_count=MAX_CONSUMED_RELATIONS)
    await verify_consumed_network_relations(connection, relations, expected_backend_pid=_PID)
    connection.fetchrow.assert_awaited_once()
    assert len(connection.fetchrow.await_args.args[1]) == MAX_CONSUMED_RELATIONS
    connection.acquire.assert_not_called()


@pytest.mark.parametrize("pid", [True, False, 0, -1, 2147483648, "42", None])
async def test_invalid_backend_pin_refuses_before_connection(pid):
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (), expected_backend_pid=pid)
    connection.is_in_transaction.assert_not_called()
    connection.fetchrow.assert_not_awaited()
    connection.acquire.assert_not_called()


@pytest.mark.parametrize("relations", [None, [], [_relation()], (_relation(),) * (MAX_CONSUMED_RELATIONS + 1)])
async def test_unbounded_or_non_tuple_selection_refuses(relations):
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, relations, expected_backend_pid=_PID)
    connection.fetchrow.assert_not_awaited()
    connection.acquire.assert_not_called()


async def test_conflicting_duplicate_names_refuse_before_query():
    relation = _relation()
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(
            connection, (relation, replace(relation, table_oid=12)), expected_backend_pid=_PID
        )
    connection.fetchrow.assert_not_awaited()


async def test_one_oid_cannot_name_two_relations():
    relation = _relation()
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(
            connection, (relation, replace(relation, table_name="another_table")), expected_backend_pid=_PID
        )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value", [("schema_name", "other.schema"), ("table_name", 'unsafe"table'), ("table_oid", True)]
)
async def test_mutated_coordinates_are_revalidated(field, value):
    relation = _relation()
    object.__setattr__(relation, field, value)
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (relation,), expected_backend_pid=_PID)
    connection.fetchrow.assert_not_awaited()


async def test_shadowed_validator_cannot_skip_oid_validation():
    relation = _relation()
    object.__setattr__(relation, "table_oid", True)
    object.__setattr__(relation, "__post_init__", lambda: None)
    connection = _connection()
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (relation,), expected_backend_pid=_PID)
    connection.fetchrow.assert_not_awaited()


async def test_other_coordinate_types_are_not_accepted():
    class OtherRelation(CompactAddressRelation):
        pass

    connection = _connection()
    relation = OtherRelation("PinnedSchema", "network_office_edge", 1)
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (relation,), expected_backend_pid=_PID)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("active", [False, 1, None])
async def test_non_transactional_connection_refuses_without_query(active):
    connection = _connection()
    connection.is_in_transaction.return_value = active
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    [
        {"backend_matches": False},
        {"backend_matches": 1},
        {"valid": False},
        {"valid": 1},
        {"relation_count": 0},
        {"relation_count": 2},
        {"relation_count": True},
    ],
)
async def test_incomplete_native_receipt_refuses(damage):
    connection = _connection(**damage)
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)
    connection.acquire.assert_not_called()


@pytest.mark.parametrize("receipt", [None, {}, {"backend_matches": True, "relation_count": 1}])
async def test_missing_native_result_refuses(receipt):
    connection = _connection()
    connection.fetchrow.return_value = receipt
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)


async def test_transaction_must_survive_the_guard():
    connection = _connection()
    connection.is_in_transaction.side_effect = [True, False]
    with pytest.raises(NetworkReadIdentityError):
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)


async def test_native_failure_cannot_expose_data():
    connection = _connection()
    failure = ConnectionError("synthetic connection failure")
    connection.fetchrow.side_effect = failure
    exposed_payloads = []
    with pytest.raises(NetworkReadIdentityError) as caught:
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)
        exposed_payloads.append("data")
    assert caught.value.__cause__ is failure and exposed_payloads == []
    connection.acquire.assert_not_called()


@pytest.mark.parametrize("changes", [{"backend_matches": False}, {"valid": False}])
async def test_consumed_payload_is_not_cached_after_refusal(changes):
    connection = _connection()
    connection.fetchrow.side_effect = [
        ["internally consumed payload"],
        {"backend_matches": True, "relation_count": 1, "valid": True} | changes,
    ]
    cached_payloads = []
    with pytest.raises(NetworkReadIdentityError):
        payload = await connection.fetchrow("synthetic actual data query")
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)
        cached_payloads.extend(payload)
    assert cached_payloads == [] and connection.fetchrow.await_count == 2
    connection.acquire.assert_not_called()


async def test_cancellation_is_preserved():
    connection = _connection()
    connection.fetchrow.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await verify_consumed_network_relations(connection, (_relation(),), expected_backend_pid=_PID)
    connection.acquire.assert_not_called()
