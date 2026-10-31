# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native physical-input checks only; fixtures supply no CMS mutation authority."""

import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from process.provider_directory_cms_prepared_context import observe_prepared_cms_inputs
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding


async def _create_input_roles(admin, role_by_kind, password, created_roles):
    """Register each exact role for teardown before attempting its creation."""
    for key, name in role_by_kind.items():
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        created_roles.append(name)
        login = "LOGIN" if key.endswith("publisher") else "NOLOGIN"
        secret = " PASSWORD " + await admin.fetchval("SELECT quote_literal($1::text)", password)
        await admin.execute(
            f'CREATE ROLE "{name}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS{secret}'
        )


async def _create_input_relations(native, role_by_kind):
    """Keep eight private inputs and the original Publisher transaction probe."""
    await native.execute(f'CREATE SCHEMA accepted_catalog AUTHORIZATION "{role_by_kind["namespace"]}"')
    await native.execute(f'CREATE SCHEMA private_source AUTHORIZATION "{role_by_kind["namespace"]}"')
    await native.execute(
        f'GRANT USAGE ON SCHEMA accepted_catalog,private_source TO "{role_by_kind["prepared_publisher"]}"'
    )
    for ordinal in range(8):
        await native.execute(f"CREATE TABLE private_source.frozen_input_{ordinal}(id integer PRIMARY KEY)")
        await native.execute(
            f'ALTER TABLE private_source.frozen_input_{ordinal} OWNER TO "{role_by_kind["prepared_owner"]}"'
        )
    await native.execute("CREATE TABLE accepted_catalog.transaction_probe(id integer)")
    await native.execute(
        f'GRANT SELECT,INSERT ON accepted_catalog.transaction_probe TO "{role_by_kind["prepared_publisher"]}"'
    )


async def _input_case(native, url, database_name, role_by_kind, password):
    """Return unchanged fixture fields from genuine native coordinates."""
    coordinate_by_field = dict(
        await native.fetchrow(
            "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
            "'accepted_catalog'::regnamespace::oid::bigint AS catalog_schema_oid,"
            "'private_source'::regnamespace::oid::bigint AS storage_schema_oid,"
            "(SELECT oid::bigint FROM pg_roles WHERE rolname=$1) AS owner_oid,"
            "(SELECT oid::bigint FROM pg_roles WHERE rolname=$2) AS namespace_owner_oid",
            role_by_kind["source_owner"],
            role_by_kind["namespace"],
        )
    )
    storage = FHIRSourceStorageBinding(
        "accepted_catalog", "private_source", **coordinate_by_field, enrollment_sha256="a" * 64
    )
    relations = [
        dict(native_row)
        for native_row in await native.fetch(
            "SELECT c.oid::bigint AS relation_oid,c.relfilenode::bigint AS relfilenode,c.relowner::bigint AS owner_oid,"
            "n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,n.nspname AS schema_name,"
            "c.relname AS relation_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='private_source' AND c.relkind='r' ORDER BY c.oid"
        )
    ]
    assert len(relations) == 8
    database = Database()
    database._database_override = database_name
    return SimpleNamespace(
        admin=native,
        database=database,
        roles=role_by_kind,
        password=password,
        url=url.set(database=database_name),
        storage=storage,
        source_policy=GenericSourcePolicy(
            "private_source", role_by_kind["source_owner"], role_by_kind["source_publisher"], (role_by_kind["reader"],)
        ),
        policy=GenericSourcePolicy(
            "private_source",
            role_by_kind["prepared_owner"],
            role_by_kind["prepared_publisher"],
            (role_by_kind["reader"],),
        ),
        inventory={"database_oid": storage.database_oid, "relations": relations},
    )


@pytest_asyncio.fixture
async def native_case():
    """Yield isolated native inputs and retire only registered random resources."""
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for the local native input-observation proof")
    url = make_url(dsn)
    assert url.host in {"localhost", "127.0.0.1"} and url.port is not None and not url.query
    token = uuid4().hex
    database_name = "cms_prepared_inputs_" + token
    role_by_kind = {
        key: "cms_inputs_" + key + "_" + token[:12]
        for key in ("source_owner", "source_publisher", "prepared_owner", "prepared_publisher", "namespace", "reader")
    }
    admin = await asyncpg.connect(dsn, command_timeout=10)
    native = None
    has_attempted_database, created_roles = False, []
    password = uuid4().hex
    try:
        assert await admin.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name)
        has_attempted_database = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        await _create_input_roles(admin, role_by_kind, password, created_roles)
        native = await asyncpg.connect(
            url.set(database=database_name).render_as_string(hide_password=False), command_timeout=10
        )
        await _create_input_relations(native, role_by_kind)
        yield await _input_case(native, url, database_name, role_by_kind, password)
    finally:
        if native is not None:
            await native.close()
        if has_attempted_database:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name)
            assert not await admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", database_name
            )
        for name in reversed(created_roles):
            await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        await admin.close()
        assert admin.is_closed()


@asynccontextmanager
async def publisher(case, *, source=False, materialize=True):
    role = case.roles["source_publisher" if source else "prepared_publisher"]
    engine = create_async_engine(
        case.url.set(drivername="postgresql+asyncpg", username=role, password=case.password),
        connect_args={"command_timeout": 10},
    )
    try:
        async with AsyncSession(bind=engine, expire_on_commit=False) as session:
            async with session.begin(), case.database.bind_existing_session(session):
                if materialize:
                    await session.execute(text("SELECT 1"))
                connection = await session.connection()
                driver = (await connection.get_raw_connection()).driver_connection
                yield session, driver
    finally:
        assert case.database._transaction_binding() is None
        await engine.dispose()


def observe(case, session, driver):
    return observe_prepared_cms_inputs(
        case.database,
        session,
        driver,
        source_binding=case.storage,
        source_policy=case.source_policy,
        prepared_policy=case.policy,
        frozen_inventory=case.inventory,
    )


async def test_native_prepared_publisher_keeps_source_owner_coordinates_separate(native_case):
    case = native_case
    source_owner = case.storage.owner_oid
    async with publisher(case) as (session, driver), observe(case, session, driver) as evidence:
        assert evidence["source_storage"]["owner_oid"] == source_owner
        assert evidence["native_coordinates"]["prepared_owner_oid"] != source_owner
        assert all(row["owner_oid"] != source_owner for row in evidence["frozen_inventory"]["relations"])
    assert case.storage.owner_oid == source_owner


async def test_native_source_publisher_is_not_a_prepared_publisher(native_case):
    case = native_case
    with pytest.raises(ValueError, match="original_publisher_required"):
        async with publisher(case, source=True) as (session, driver), observe(case, session, driver):
            pytest.fail("Source publisher entered the prepared observation")


async def test_native_unmaterialized_session_is_not_a_physical_transaction(native_case):
    case = native_case
    with pytest.raises(ValueError, match="physical_connection_changed"):
        async with publisher(case, materialize=False) as (session, driver), observe(case, session, driver):
            pytest.fail("virtual SQLAlchemy transaction was accepted as native")


@pytest.mark.parametrize("change", ["owner", "relfilenode"])
async def test_native_concurrent_physical_change_is_observed_at_exit(native_case, change):
    case = native_case
    with pytest.raises(ValueError, match="frozen_inventory_changed"):
        async with publisher(case) as (session, driver), observe(case, session, driver):
            if change == "owner":
                await case.admin.execute(
                    f'ALTER TABLE private_source.frozen_input_0 OWNER TO "{case.roles["source_owner"]}"'
                )
            else:
                await case.admin.execute("TRUNCATE TABLE private_source.frozen_input_0")


async def test_native_inherited_task_cannot_borrow_prepared_session(native_case):
    case = native_case
    async with publisher(case) as (session, driver):

        async def child():
            async with observe(case, session, driver):
                pytest.fail("inherited session reached the body")

        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.create_task(child())


@pytest.mark.parametrize("cancel", [False, True])
async def test_native_caller_failure_or_cancellation_rolls_back_its_transaction(native_case, cancel):
    case = native_case
    entered = asyncio.Event()

    async def work():
        async with publisher(case) as (session, driver), observe(case, session, driver):
            await session.execute(text("INSERT INTO accepted_catalog.transaction_probe VALUES(1)"))
            entered.set()
            if cancel:
                await asyncio.Event().wait()
            raise RuntimeError("caller failure")

    if cancel:
        task = asyncio.create_task(work())
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    else:
        with pytest.raises(RuntimeError, match="caller failure"):
            await work()
    assert await case.admin.fetchval("SELECT count(*) FROM accepted_catalog.transaction_probe") == 0
