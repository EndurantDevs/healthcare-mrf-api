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
    """Keep fixture actor, custody and frozen-row metadata as separate mutable objects."""
    origin_by_field = dict(
        session=policy.publisher_role,
        current=policy.publisher_role,
        backend=41,
        database_oid=71,
        readonly="off",
        isolation="read committed",
        replication_role="origin",
        transaction="3/4",
    )
    coordinates_by_field = dict(
        catalog_schema_oid=72,
        storage_schema_oid=73,
        namespace_owner_oid=75,
        source_owner_oid=74,
        prepared_owner_oid=76,
        source_owner_login=False,
        source_publisher_login=True,
        prepared_owner_login=False,
        prepared_publisher_login=True,
    )
    relations = [
        dict(
            schema_name="private_source",
            relation_name=f"frozen_input_{n}",
            relation_oid=100 + n,
            schema_oid=73,
            schema_owner_oid=75,
            owner_oid=76,
            relfilenode=200 + n,
        )
        for n in range(8)
    ]
    inventory_by_field = dict(database_oid=71, relations=relations)
    native_rows = [{**relation, "supported": True} for relation in relations]
    return origin_by_field, coordinates_by_field, inventory_by_field, native_rows


@pytest_asyncio.fixture
async def case():
    """Provide one mock physical driver while retaining real session and teardown boundaries."""
    engine = create_async_engine("postgresql+asyncpg://prepared_publisher@localhost/synthetic")
    database = Database()
    database._database_override = "synthetic"
    source_policy = source.GenericSourcePolicy("private_source", "source_owner", "source_publisher", ("reader",))
    policy = source.GenericSourcePolicy("private_source", "prepared_owner", "prepared_publisher", ("reader",))
    storage = FHIRSourceStorageBinding("catalog", "private_source", 71, 72, 73, 74, 75, "a" * 64)
    origin_by_field, coordinates_by_field, inventory_by_field, native_rows = _prepared_input_metadata(policy)

    async def fetchrow(query, *arguments):
        if query == prepared._ORIGIN_SQL:
            return origin_by_field.copy()
        assert query == prepared._COORDINATES_SQL
        return coordinates_by_field.copy()

    driver = SimpleNamespace(
        is_closed=Mock(return_value=False),
        is_in_transaction=Mock(return_value=True),
        get_server_pid=Mock(return_value=41),
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetch=AsyncMock(side_effect=lambda *args: deepcopy(native_rows)),
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


async def test_observes_separate_owners_without_context_or_transaction_changes(case):
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        async with observe(case) as evidence:
            assert evidence["source_storage"]["owner_oid"] == 74
            assert evidence["native_coordinates"]["prepared_owner_oid"] == 76
            assert evidence["frozen_inventory"] == case.inventory
            evidence["frozen_inventory"]["relations"].clear()
            assert case.storage.owner_oid == 74 and len(case.inventory["relations"]) == 8
        assert case.session.get_transaction() is transaction
        assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
    assert case.driver.fetch.await_count == 2
    assert not hasattr(prepared, "finalize_frozen_cms_source_candidate")
    assert not hasattr(prepared, "bind_generic_source_publisher")


@pytest.mark.parametrize(
    "field,value",
    [
        ("session", "source_publisher"),
        ("current", "prepared_owner"),
        ("database_oid", 99),
        ("backend", 99),
        ("readonly", "on"),
        ("replication_role", "replica"),
        ("transaction", None),
    ],
)
async def test_wrong_native_origin_fails_before_inventory(case, field, value):
    case.origin[field] = value
    with pytest.raises(ValueError, match="original_publisher_required"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("wrong original actor reached the body")
    case.driver.fetch.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_owner_oid", 76),
        ("prepared_owner_oid", 74),
        ("storage_schema_oid", 99),
        ("catalog_schema_oid", 99),
        ("namespace_owner_oid", 99),
        ("prepared_owner_login", True),
    ],
)
async def test_source_binding_cannot_be_reinterpreted_as_prepared_custody(case, field, value):
    case.coordinates[field] = value
    with pytest.raises(ValueError, match="native_coordinates_changed"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("incorrect native custody reached the body")
    case.driver.fetch.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_oid", 74),
        ("relation_oid", 999),
        ("relfilenode", 999),
        ("relation_name", "replaced_input"),
        ("schema_owner_oid", 999),
        ("supported", False),
    ],
)
async def test_native_frozen_identity_or_owner_drift_is_rejected(case, field, value):
    case.native_rows[0][field] = value
    with pytest.raises(ValueError, match="frozen_inventory_changed"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("changed input reached the body")


async def test_bridge_and_original_driver_are_required_before_native_io(case):
    async with case.session.begin():
        with pytest.raises(ValueError, match="borrowed_session_required"):
            async with observe(case):
                pytest.fail("unbound caller was accepted")
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        case.physical.get_raw_connection.return_value.driver_connection = object()
        with pytest.raises(ValueError, match="physical_connection_changed"):
            async with observe(case):
                pytest.fail("replaced driver was accepted")
    case.driver.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("change", ["inventory", "origin", "driver", "ended_transaction"])
async def test_exit_rechecks_inputs_and_original_physical_transaction(case, change):
    with pytest.raises(
        ValueError,
        match="inputs_changed|native_evidence_changed|physical_connection_changed|original_transaction_required",
    ):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            match change:
                case "inventory":
                    case.inventory["relations"][0]["relfilenode"] = 999
                case "origin":
                    case.origin["transaction"] = "3/5"
                case "driver":
                    case.physical.get_raw_connection.return_value.driver_connection = object()
                case _:
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
    assert case.driver.fetch.await_count == 1


async def test_closed_inventory_refuses_extra_or_source_owned_input_before_native_sql(case):
    case.inventory["relations"][0]["owner_oid"] = case.storage.owner_oid
    with pytest.raises(ValueError, match="frozen_coordinates_changed"):
        async with case.session.begin(), case.database.bind_existing_session(case.session), observe(case):
            pytest.fail("Source-owned input was accepted as frozen")
    case.driver.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("change", ["count", "duplicate_oid", "duplicate_name", "extra_field", "boolean_oid", "schema"])
async def test_malformed_frozen_inventory_is_refused_before_native_io(case, change):
    relations = case.inventory["relations"]
    match change:
        case "count":
            relations.pop()
        case "duplicate_oid":
            relations[0]["relation_oid"] = relations[1]["relation_oid"]
        case "duplicate_name":
            relations[0]["relation_name"] = relations[1]["relation_name"]
        case "extra_field":
            relations[0]["authority"] = True
        case "boolean_oid":
            relations[0]["relation_oid"] = True
        case _:
            relations[0]["schema_name"] = "catalog"
    with pytest.raises(ValueError, match="inventory_invalid|frozen_coordinates_changed"):
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
