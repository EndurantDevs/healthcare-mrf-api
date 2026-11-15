# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared input observations cannot create SOURCE or CMS write authority."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from process import provider_directory_cms_prepared_context as prepared
from process import provider_directory_generic_source_policy as source
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding


def _prepared_input_metadata(policy):
    """Separate configured roles, transaction modes and portable input names."""
    origin_by_field = dict(
        current=policy.publisher_role, readonly="off", isolation="read committed", replication_role="origin"
    )
    relations = [dict(schema_name="private_source", relation_name=f"frozen_input_{n}") for n in range(8)]
    inventory_by_field = {"relations": relations}
    return origin_by_field, inventory_by_field, inventory_by_field, relations


@pytest_asyncio.fixture
async def case():
    """Provide one mock physical driver while retaining real session and teardown boundaries."""
    engine = create_async_engine("postgresql+asyncpg://prepared_publisher@localhost/synthetic")
    database = Database()
    database._database_override = "synthetic"
    source_policy = source.GenericSourcePolicy("private_source", "source_owner", "source_publisher", ("reader",))
    policy = source.GenericSourcePolicy("private_source", "prepared_owner", "prepared_publisher", ("reader",))
    storage = FHIRSourceStorageBinding("catalog", "private_source")
    origin_by_field, coordinates_by_field, inventory_by_field, native_rows = _prepared_input_metadata(policy)

    async def fetchrow(query, *arguments):
        if query == prepared._ORIGIN_SQL:
            return origin_by_field.copy()
        raise AssertionError(query)

    driver = SimpleNamespace(
        is_closed=Mock(return_value=False),
        is_in_transaction=Mock(return_value=True),
        get_server_pid=Mock(return_value=41),
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetch=AsyncMock(side_effect=lambda *args: deepcopy(native_rows)),
        execute=AsyncMock(),
    )
    physical = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    async with AsyncSession(bind=engine) as session:
        session.connection = AsyncMock(return_value=physical)
        try:
            yield SimpleNamespace(
                database=database,
                session=session,
                driver=driver,
                physical=physical,
                storage=storage,
                source_policy=source_policy,
                policy=policy,
                origin=origin_by_field,
                coordinates=coordinates_by_field,
                inventory=inventory_by_field,
                native_rows=native_rows,
            )
        finally:
            assert database._transaction_binding() is None
    await engine.dispose()


def observe(case, **changes):
    arguments_by_name = dict(
        source_binding=case.storage,
        source_policy=case.source_policy,
        prepared_policy=case.policy,
        frozen_inventory=case.inventory,
    )
    arguments_by_name.update(changes)
    return prepared.observe_prepared_cms_inputs(
        case.database,
        case.session,
        case.driver,
        **arguments_by_name,
    )


async def test_observes_complete_named_inputs_without_transaction_or_context_changes(case):
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        async with observe(case) as evidence:
            assert evidence["source_storage"] == {"catalog_schema": "catalog", "storage_schema": "private_source"}
            assert evidence["frozen_inventory"] == case.inventory
            evidence["frozen_inventory"]["relations"].clear()
            assert len(case.inventory["relations"]) == 8
        assert case.session.get_transaction() is transaction
        assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
    assert case.driver.execute.await_count == 16
    assert all(
        str(call.args[0]).startswith('SELECT 1 FROM "private_source".') for call in case.driver.execute.await_args_list
    )
    case.driver.fetch.assert_not_awaited()


@pytest.mark.parametrize("field,value", [("readonly", "on"), ("isolation", "invalid"), ("replication_role", "replica")])
async def test_invalid_preparation_transaction_refuses_before_reading_inputs(case, field, value):
    case.origin[field] = value
    with pytest.raises(ValueError, match="writable_transaction_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("invalid transaction reached the body")
    case.driver.execute.assert_not_awaited()


async def test_missing_or_inaccessible_named_input_propagates_native_refusal(case):
    case.driver.execute.side_effect = PermissionError("input read refused")
    with pytest.raises(PermissionError, match="input read refused"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("unreadable input reached the body")


async def test_borrowed_session_is_required_before_native_io(case):
    async with case.session.begin():
        with pytest.raises(ValueError, match="borrowed_session_required"):
            async with observe(case):
                pytest.fail("unbound caller was accepted")
    case.driver.fetchrow.assert_not_awaited()


async def test_virtual_session_transaction_is_materialized_before_raw_input_reads(case):
    native_state_by_field = {"active": False}
    case.driver.is_in_transaction.side_effect = lambda: native_state_by_field["active"]

    async def materialize(statement):
        assert str(statement) == "SELECT 1"
        native_state_by_field["active"] = True

    case.session.execute = AsyncMock(side_effect=materialize)
    async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
        assert native_state_by_field["active"]
    case.session.execute.assert_awaited_once()


@pytest.mark.parametrize("change", ["inventory", "ended_transaction"])
async def test_exit_rechecks_logical_inputs_and_transaction_lifetime(case, change):
    with pytest.raises(ValueError, match="inputs_changed|original_transaction_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            if change == "inventory":
                case.inventory["relations"][0]["relation_name"] = "replacement"
            else:
                await case.session.rollback()


async def test_inherited_task_and_nested_transaction_are_refused(case):
    async def child():
        async with observe(case):
            pytest.fail("child task inherited prepared session")

    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.create_task(child())
        async with case.session.begin_nested():
            with pytest.raises(ValueError, match="original_transaction_required"):
                async with observe(case):
                    pytest.fail("nested transaction was accepted")
    case.driver.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("error", [RuntimeError("caller failure"), asyncio.CancelledError()])
async def test_caller_failure_keeps_rollback_and_cleanup_with_caller(case, error):
    with pytest.raises(type(error)):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            raise error
    assert not case.session.in_transaction()
    assert case.driver.execute.await_count == 8


@pytest.mark.parametrize("change", ["count", "duplicate_name", "extra_field", "schema", "invalid_name"])
async def test_malformed_named_inventory_is_refused_before_native_io(case, change):
    relations = case.inventory["relations"]
    if change == "count":
        relations.pop()
    elif change == "duplicate_name":
        relations[0]["relation_name"] = relations[1]["relation_name"]
    elif change == "extra_field":
        relations[0]["authority"] = True
    elif change == "schema":
        relations[0]["schema_name"] = "catalog"
    else:
        relations[0]["relation_name"] = "unsafe.name"
    with pytest.raises(ValueError, match="inventory_invalid|frozen_coordinates_changed|identifier_invalid"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("malformed input reached native IO")
    case.driver.fetchrow.assert_not_awaited()


async def test_source_policy_and_duck_bindings_refuse_before_native_io(case):
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="roles_overlap"):
            async with observe(case, source_policy=case.policy):
                pytest.fail("prepared policy replaced Source policy")
        with pytest.raises(ValueError, match="explicit_coordinates_required"):
            async with observe(case, source_binding=SimpleNamespace(**vars(case.storage))):
                pytest.fail("detached coordinates replaced typed Source binding")
    case.driver.fetchrow.assert_not_awaited()
