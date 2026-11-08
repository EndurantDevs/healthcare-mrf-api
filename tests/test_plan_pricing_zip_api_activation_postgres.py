# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ZIP setup guards only; these fixtures grant no published Source authority."""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import network_read_execution as execution
from api import plan_pricing_code_build as build
from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_read as projection
from api import ptg2_geo_projection as geo
from api import ptg2_tables
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from tests.test_plan_pricing_code_projection import _selection


@pytest.fixture
async def zip_api_db(monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated task database")
    admin = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    schema, tiger = "zip_api_" + suffix, "zip_geo_" + suffix
    owner, reader = "zip_owner_" + suffix, "zip_reader_" + suffix
    password, engine = uuid4().hex + uuid4().hex, None
    try:
        await admin.execute(
            f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        await admin.execute(
            f"CREATE ROLE \"{reader}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        monkeypatch.setattr(build, "SCHEMA", schema)
        monkeypatch.setattr(projection_contract, "SCHEMA", schema)
        monkeypatch.setattr(ptg2_tables, "PTG2_SCHEMA", schema)
        monkeypatch.setattr(
            geo,
            "_PROJECTION_DEPENDENCIES",
            tuple(
                (tiger if dependency_schema else None, name) for dependency_schema, name in geo._PROJECTION_DEPENDENCIES
            ),
        )
        await _setup_native_inputs(admin, schema, tiger, owner, reader)
        url = make_url(dsn).set(drivername="postgresql+asyncpg", username=reader, password=password)
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        yield SimpleNamespace(
            admin=admin, schema=schema, reader=reader, sessions=async_sessionmaker(engine, expire_on_commit=False)
        )
    finally:
        if engine is not None:
            await engine.dispose()
        for namespace in (tiger, schema):
            await admin.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
        for role in (reader, owner):
            if await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
                await admin.execute(f'DROP OWNED BY "{role}"')
                await admin.execute(f'DROP ROLE "{role}"')
        await admin.close()


async def _setup_native_inputs(admin, schema, tiger, owner, reader):
    for namespace in (schema, tiger):
        await admin.execute(f'CREATE SCHEMA "{namespace}" AUTHORIZATION "{owner}"')
    names = {
        (schema, name)
        for name in (*build.PROVIDER_RELATIONS, *build._API_SOURCE_NAMES, ptg2_tables.PTG2_V4_GRAPH_DIAGNOSTIC_TABLE)
    }
    names.update((dependency_schema or schema, name) for dependency_schema, name in geo._PROJECTION_DEPENDENCIES)
    columns_by_name = {
        "ptg2_snapshot": "snapshot_id text PRIMARY KEY,manifest jsonb",
        "ptg2_v3_snapshot_binding": "snapshot_id text,snapshot_key bigint",
        "ptg2_v3_snapshot_layout": "snapshot_key bigint,layout_manifest jsonb",
        "geo_zip_lookup": "zip_code text,latitude double precision,longitude double precision",
    }
    for namespace, name in sorted(names):
        await admin.execute(f'CREATE TABLE "{namespace}"."{name}" ({columns_by_name.get(name, "marker text")})')
        await admin.execute(f'ALTER TABLE "{namespace}"."{name}" OWNER TO "{owner}"')
    await admin.execute(f"INSERT INTO \"{schema}\".ptg2_snapshot VALUES('synthetic-snapshot','{{}}')")
    await admin.execute(f"INSERT INTO \"{schema}\".ptg2_v3_snapshot_binding VALUES('synthetic-snapshot',1)")
    await admin.execute(f"INSERT INTO \"{schema}\".ptg2_v3_snapshot_layout VALUES(1,'{{}}')")
    await admin.execute(f"INSERT INTO \"{schema}\".geo_zip_lookup VALUES('48104',42.27,-83.74),('48105',42.3,-83.7)")
    for namespace in (schema, tiger):
        await admin.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{reader}"')
        await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{reader}"')


async def test_native_selected_read_keeps_original_context_without_eager_provider_lock(zip_api_db):
    fixture = zip_api_db
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE "{fixture.schema}".npi IN ACCESS EXCLUSIVE MODE NOWAIT')
        async with fixture.sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='1000ms'"))
            driver, pid = await execution._original_read_driver(session)
            transaction = session.get_transaction()
            assert await driver.fetchval("SELECT session_user=current_user AND session_user=$1", fixture.reader)
            async with build._api_input_reader(session, _selection()) as reader:
                relation = reader.source_relation(fixture.schema, "geo_zip_lookup")
                result = await reader.execute(text(f"SELECT zip_code FROM {relation} ORDER BY zip_code LIMIT 1"))
                assert result.scalar_one() == "48104"
            current, current_pid = await execution._original_read_driver(session)
            assert current is driver and current_pid == pid and session.get_transaction() is transaction
            assert await driver.fetchval(
                "SELECT NOT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND relation=to_regclass($1)::oid AND locktype='relation')",
                f'"{fixture.schema}".npi',
            )


async def test_native_geo_read_excludes_child_attached_after_metadata(zip_api_db):
    fixture = zip_api_db
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout='1000ms'"))
        async with build._api_input_reader(session, _selection()) as reader:
            await fixture.admin.execute(
                f'CREATE TABLE "{fixture.schema}".late_geo_child () INHERITS ("{fixture.schema}".geo_zip_lookup)'
            )
            async with fixture.admin.transaction():
                await fixture.admin.execute(
                    f'LOCK TABLE "{fixture.schema}".late_geo_child IN ACCESS EXCLUSIVE MODE NOWAIT'
                )
                cells = await projection.geo_cells(
                    reader,
                    {"zip5": "48104", "zip_radius_miles": "25"},
                    result_type="rate_aggregates",
                    api_read=True,
                )
                assert sorted(cells) == ["48104", "48105"]
            driver, _ = await execution._original_read_driver(session)
            assert await driver.fetchval(
                "SELECT NOT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND relation=to_regclass($1)::oid AND locktype='relation')",
                f'"{fixture.schema}".late_geo_child',
            )


@pytest.mark.parametrize(
    "statement",
    [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE",
    ],
)
async def test_native_api_setup_refuses_an_inappropriate_caller_transaction(zip_api_db, statement):
    async with zip_api_db.sessions() as session, session.begin():
        await session.execute(text(statement))
        with pytest.raises(PlanPricingProjectionUnavailable, match="read-only"):
            async with build._api_input_reader(session, _selection()):
                pytest.fail("invalid transaction reached setup")
        assert execution._SESSION_INVENTORY_KEY not in session.info


@pytest.mark.parametrize("declaration", ["physical_binding_contract", "local_data_preparation", "layout"])
async def test_native_local_source_refusal_precedes_unqualified_source_reader(zip_api_db, declaration):
    fixture = zip_api_db
    if declaration == "layout":
        await fixture.admin.execute(
            f'UPDATE "{fixture.schema}".ptg2_v3_snapshot_layout SET layout_manifest=\'{{"physical_binding":{{}}}}\''
        )
    else:
        await fixture.admin.execute(
            f"UPDATE \"{fixture.schema}\".ptg2_snapshot SET manifest=jsonb_build_object($1::text,'unqualified')",
            declaration,
        )
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        with pytest.raises(PlanPricingProjectionUnavailable, match="selected source"):
            await build.resolve_inputs(session, _selection(), ("CPT", "27447"), api_read=True)
        assert execution._SESSION_INVENTORY_KEY not in session.info


async def test_native_provider_select_privilege_remains_required(zip_api_db):
    fixture = zip_api_db
    await fixture.admin.execute(f'REVOKE SELECT ON "{fixture.schema}".npi FROM "{fixture.reader}"')
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        with pytest.raises(PlanPricingProjectionUnavailable, match="provider inputs"):
            async with build._api_input_reader(session, _selection()):
                pytest.fail("missing SELECT privilege reached setup")
        assert execution._SESSION_INVENTORY_KEY not in session.info


async def test_native_setup_cancellation_restores_request_inventory(zip_api_db):
    async with zip_api_db.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        with pytest.raises(asyncio.CancelledError):
            async with build._api_input_reader(session, _selection()):
                raise asyncio.CancelledError
        assert execution._SESSION_INVENTORY_KEY not in session.info
