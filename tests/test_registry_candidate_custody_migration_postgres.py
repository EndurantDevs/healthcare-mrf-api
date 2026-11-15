# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Missing-ledger forward preparation through genuine restricted native identities."""

from contextlib import asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_network_candidate_custody import REVISION
from process.registry_candidate_custody_preparation import prepare_registry_candidate_custody
from process.registry_management_permissions import (
    RegistryManagementPermissionError,
    install_registry_office_recipe_permissions,
)
from tests.test_manual_provider_identity_store_postgres import provider_db as provider_db
from tests.test_manual_provider_identity_store_postgres import serving_schema as serving_schema
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_registry_management_permissions_postgres import permissions_db as permissions_db
from tests.test_registry_prepared_migrations_postgres import (
    _CHANGES,
    _acl,
    _arguments,
    _maintenance,
    _upgrade,
)
from tests.test_registry_prepared_migrations_postgres import prepared_db as prepared_db

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def custody_db(prepared_db):
    fixture = prepared_db
    # Model the committed four changes, with the genuinely missing fifth relation.
    for revision, _ in _CHANGES:
        if revision != REVISION:
            await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", f'"{fixture.schema}".network_candidate_custody')
        is None
    )
    fixture.operator = await _maintenance(fixture)
    return fixture


@asynccontextmanager
async def _operator(fixture, *, ordinary=False):
    role = fixture.roles_by_kind["api"] if ordinary else fixture.operator
    password = fixture.api_password if ordinary else fixture.operator_password
    engine = create_async_engine(fixture.sessions.kw["bind"].url.set(username=role, password=password))
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL statement_timeout='3s'"))
            await connection.execute(text("SET LOCAL lock_timeout='500ms'"))
            assert await connection.scalar(text("SELECT session_user=current_user")) is True
            yield connection
            assert await connection.scalar(text("SELECT current_user")) == role
            assert await connection.scalar(text("SELECT to_regclass('pg_temp.network_candidate_custody')")) is None
    finally:
        await engine.dispose()


async def _forward(connection, fixture):
    return await prepare_registry_candidate_custody(
        connection, owner_role=fixture.roles_by_kind["owner"], control_schema=fixture.schema
    )


async def _incumbents(fixture):
    return {
        tuple(row.values())
        for row in await fixture.connection.fetch(
            "SELECT c.oid,c.relname,c.relowner,c.relacl::text FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1",
            fixture.schema,
        )
    }


async def test_missing_custody_forward_only_then_adoption_and_narrow_enrollment(custody_db):
    fixture = custody_db
    with pytest.raises(RuntimeError, match="preparation is missing"):
        await _upgrade(fixture, REVISION, fixture.roles_by_kind["api"])
    before, grants = await _incumbents(fixture), await _acl(fixture)
    async with _operator(fixture) as connection:
        receipt = await _forward(connection, fixture)
        assert receipt["created"] and receipt["prepared_revisions"] == [REVISION]
        # The parent remains available to an already-authorized reader during preparation.
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            await fixture.connection.fetch(f'SELECT candidate_id FROM "{fixture.schema}".network_membership_candidate')
    assert before <= await _incumbents(fixture)
    assert {grant for grant in await _acl(fixture) if grant[0] != "network_candidate_custody"} == grants
    retained, retained_grants = await _incumbents(fixture), await _acl(fixture)
    async with _operator(fixture) as connection:
        assert (await _forward(connection, fixture)) == {**receipt, "created": False}
    assert (await _incumbents(fixture), await _acl(fixture)) == (retained, retained_grants)
    candidate = uuid4()
    await _candidate(fixture.connection, fixture.schema, candidate)
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".network_candidate_custody(candidate_id,seal_sha256,receipt_json) VALUES($1,$2,$3::jsonb)',
        candidate,
        "a" * 64,
        '{"fixture":true}',
    )
    retained_rows = await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}".network_candidate_custody')
    await _upgrade(fixture, REVISION, fixture.roles_by_kind["api"])
    await _upgrade(fixture, REVISION, fixture.roles_by_kind["api"])
    assert await _incumbents(fixture) == retained and await _acl(fixture) == retained_grants
    assert (
        await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}".network_candidate_custody') == retained_rows
    )
    async with _operator(fixture) as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        await install_registry_office_recipe_permissions(driver, **_arguments(fixture))
    after = await _acl(fixture)
    owner_oid, api_oid = await fixture.connection.fetchrow(
        "SELECT (SELECT oid FROM pg_roles WHERE rolname=$1),(SELECT oid FROM pg_roles WHERE rolname=$2)",
        fixture.roles_by_kind["owner"],
        fixture.roles_by_kind["api"],
    )
    assert after - retained_grants == {
        ("registry_network_binding", "office_recipe_json", owner_oid, api_oid, privilege, False)
        for privilege in ("INSERT", "UPDATE")
    }
    assert retained_grants <= after
    assert not await fixture.connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'SET')", fixture.roles_by_kind["api"], fixture.roles_by_kind["owner"]
    )


async def test_custody_preparation_refuses_ordinary_owner_and_active_candidate_writer(custody_db):
    fixture = custody_db
    before = await _incumbents(fixture), await _acl(fixture)
    async with _operator(fixture, ordinary=True) as connection:
        # PostgreSQL itself refuses the ordinary operator's switch to the owner role.
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="permission denied to set role"):
            await _forward(connection, fixture)
    async with fixture.connection.transaction():
        await fixture.connection.execute(
            f'LOCK TABLE "{fixture.schema}".network_membership_candidate IN ROW EXCLUSIVE MODE'
        )
        async with _operator(fixture) as connection:
            with pytest.raises(DBAPIError, match="lock"):
                await _forward(connection, fixture)
    assert (await _incumbents(fixture), await _acl(fixture)) == before


async def test_custody_preparation_rollback_removes_only_its_new_objects(custody_db):
    fixture = custody_db
    before = await _incumbents(fixture), await _acl(fixture)
    with pytest.raises(RuntimeError, match="after preparation"):
        async with _operator(fixture) as connection:
            await _forward(connection, fixture)
            raise RuntimeError("after preparation")
    assert (await _incumbents(fixture), await _acl(fixture)) == before
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", f'"{fixture.schema}".network_candidate_custody')
        is None
    )
