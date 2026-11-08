# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native managed E&M SQL proof; published resolution and HTTP are separate."""

import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import network_address_scope as address_scope
from api import plan_pricing_em_attribution as attribution
from api import plan_pricing_em_distance as distance
from api import plan_pricing_em_managed as managed
from api.network_read_execution import install_network_read_inventory
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from api.ptg2_serving import PTG2LocationScopeError
from db.models import EntityAddressUnified
from process import network_em_provider_inputs as provider_inputs
from process.network_compact_layout import CompactAddressRelation
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_plan_pricing_projection_postgres import POSTGRES_DSN_ENV, _sqlalchemy_async_dsn
from tests.test_plan_pricing_projection_v3_differential_postgres import migrated_v3_database as migrated_v3_database

pytestmark = pytest.mark.asyncio


def _selection():
    bindings = tuple(
        PlanReleaseSnapshotBinding(n, f"snapshot_{n}", f"source_{n}", "example_plan", "group", "in_network", True)
        for n in (3, 17, 29, 41, 42)
    )
    return PlanReleaseServingSelection(
        "hpserve_" + "1" * 26,
        "hprelease_" + "2" * 26,
        "hpplan_" + "3" * 26,
        None,
        "2026-08",
        "published",
        "a" * 64,
        bindings,
    )


async def _held_tables(connection, schema):
    relations = []
    for index, role in enumerate(provider_inputs._ROLES):
        table = "held_" + str(index)
        columns_by_name = provider_inputs._required_columns(role)
        columns = []
        for name, kind in columns_by_name.items():
            native_type = kind[1:] + "[]" if kind.startswith("_") else kind
            if kind == "geometry":
                native_type = "geometry(MultiPolygon,4269)"
            columns.append(f'"{name}" {native_type}')
        await connection.execute(f'CREATE TABLE "{schema}"."{table}" ({",".join(columns)})')
        key = {"npi": "npi", "npi_taxonomy": "npi,checksum", "nucc_taxonomy": "code", "geo_zip_lookup": "zip_code"}.get(
            role
        )
        if role == "npi_address":
            await connection.execute(f'CREATE INDEX ON "{schema}"."{table}" (npi,address_key)')
        if key:
            await connection.execute(f'CREATE UNIQUE INDEX ON "{schema}"."{table}" ({key})')
        oid, filenode = await connection.fetchrow(
            "SELECT oid::bigint,pg_relation_filenode(oid)::bigint FROM pg_class WHERE oid=to_regclass($1)",
            f'"{schema}"."{table}"',
        )
        relations.append(
            provider_inputs.EMProviderInputRelation(role, schema, table, "example_generation", oid, filenode)
        )
    return provider_inputs.EMProviderInputs(tuple(relations))


async def _seed_dependencies(connection, inputs):
    await connection.execute(
        f"INSERT INTO {inputs.relation('geo_zip_lookup')}(zip_code,state,state_name,latitude,longitude) VALUES('60611','IL','Illinois',41.9,-87.65)"
    )
    await connection.execute(f"INSERT INTO {inputs.relation('tiger.zip_state')}(zip,stusps) VALUES('60611','IL')")
    await connection.execute(
        f"INSERT INTO {inputs.relation('tiger.zcta5')}(zcta5ce,the_geom) VALUES('60611',ST_Multi(ST_MakeEnvelope(-87.7,41.85,-87.6,41.95,4269)))"
    )
    await connection.execute(
        f"INSERT INTO {inputs.relation('npi')}(npi,entity_type_code,provider_first_name,provider_last_name) SELECT n,1,'Example','Provider' FROM unnest($1::bigint[]) n",
        [101, 102, 103, 104, 105, 106, 107, 108, 201],
    )
    await connection.execute(
        f"UPDATE {inputs.relation('npi')} SET provider_organization_name=$1 WHERE npi=108", "X" * (9 * 1024 * 1024)
    )


def _office(npi, name, latitude, *, origin="base", state="IL", evidence=True):
    return {
        "entity_type": "npi",
        "entity_id": str(npi),
        "npi": npi,
        "location_key": uuid4().hex,
        "row_origin": origin,
        "address_precision": "rooftop",
        "checksum": int(latitude * 1_000_000),
        "type": "practice",
        "first_line": "10 Example Street",
        "city_name": name,
        "state_name": state,
        "postal_code": "60611",
        "country_code": "US",
        "address_key": uuid4(),
        "premise_key": uuid4(),
        "zip5": "60611",
        "state_code": "IL",
        "lat": Decimal(str(latitude)),
        "long": Decimal("-87.65"),
        "address_source_mask": 1,
        "canonical_network_ids": [7],
        "evidence": evidence,
    }


async def _seed_offices(connection, address_schema, inputs):
    offices = [
        _office(101, "DENIED SOURCE", 41.9001),
        _office(102, "DENIED OFFICE A", 41.9002, origin="network_custom"),
        _office(102, "ALLOWED OFFICE B", 41.91),
        _office(103, "OVERLAP PUBLIC", 41.92),
        _office(104, "INVALID IDENTITY", 41.9003, state="NY"),
        _office(105, "NO EVIDENCE", 41.9004, evidence=False),
        _office(106, "OUTSIDE ZIP POLYGON", 42.0),
        _office(107, "COPIED PUBLIC", 41.91),
    ]
    offices.extend(_office(201, "DENSE OFFICE", 41.93 + index / 10_000_000) for index in range(8200))
    names = tuple(name for name in offices[0] if name != "evidence")
    values = [tuple(office[name] for name in names) for office in offices]
    await connection.copy_records_to_table(
        "entity_address_unified", schema_name=address_schema, columns=names, records=values
    )
    await connection.copy_records_to_table(
        inputs.relations[4].table_name,
        schema_name=inputs.relations[4].schema_name,
        columns=("npi", "address_key", "date_added"),
        records=[(office["npi"], office["address_key"], date(2026, 8, 1)) for office in offices if office["evidence"]],
    )


async def _publish_rates(connection, selection, owner, reader):
    await connection.execute(
        "CREATE TEMP TABLE plan_pricing_em_distance_rate_stage(binding_ordinal integer,npi bigint,code_index smallint,minimum_rate numeric,maximum_rate numeric,rate_count bigint)"
    )
    source_rows = [
        (3, 101, "1", "2", 2),
        (3, 102, "1", "2", 50),
        (17, 102, "100.125", "110.5", 5),
        (17, 103, "90", "120", 3),
        (29, 103, "80", "100", 7),
        (17, 104, "1", "2", 1),
        (17, 105, "1", "2", 1),
        (17, 106, "1", "2", 1),
        (17, 107, "70", "80", 1),
        (41, 201, "10", "20", 1),
        (42, 108, "1", "2", 1),
    ]
    rows = [
        (ordinal, npi, index, Decimal(low), Decimal(high), count)
        for ordinal, npi, low, high, count in source_rows
        for index in range(6)
    ]
    await connection.copy_records_to_table("plan_pricing_em_distance_rate_stage", records=rows)
    return await attribution._publish(connection, selection, attribution._identity(selection), owner, (reader,))


async def _close_schema(connection, schema, owner, reader):
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"')
    tables = await connection.fetch("SELECT tablename FROM pg_tables WHERE schemaname=$1", schema)
    for table in tables:
        await connection.execute(f'ALTER TABLE "{schema}"."{table["tablename"]}" OWNER TO "{owner}"')
    await connection.execute(
        f'REVOKE ALL ON SCHEMA "{schema}" FROM PUBLIC; GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"'
    )
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"')


async def _prepare(connection, fixture, owner, reader):
    await connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    held_schema = "em_inputs_" + uuid4().hex
    address_id = uuid4()
    address_schema = "network_candidate_" + address_id.hex
    await connection.execute(f'CREATE SCHEMA "{held_schema}"; CREATE SCHEMA "{address_schema}"')
    inputs = await _held_tables(connection, held_schema)
    columns = []
    from sqlalchemy.dialects.postgresql import dialect

    for column in EntityAddressUnified.__table__.columns:
        columns.append(f'"{column.name}" {column.type.compile(dialect=dialect())}')
    await connection.execute(
        f'CREATE TABLE "{address_schema}".entity_address_unified ({",".join(columns)},PRIMARY KEY(location_key))'
    )
    await connection.execute(
        f"CREATE INDEX office_knn ON \"{address_schema}\".entity_address_unified USING gist(public.Geography(public.ST_MakePoint((long)::double precision,(lat)::double precision))) WHERE type IN ('primary','secondary','practice','site') AND coalesce(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL"
    )
    await connection.execute(
        f'CREATE TABLE "{fixture.schema}".registry_approved_record(approved_revision bigint,record_kind text,record_key text,record_json jsonb)'
    )
    await _seed_dependencies(connection, inputs)
    await _seed_offices(connection, address_schema, inputs)
    selection = _selection()
    receipt = await _publish_rates(connection, selection, owner, reader)
    for schema in (held_schema, address_schema):
        await _close_schema(connection, schema, owner, reader)
    await connection.execute(
        f'GRANT USAGE ON SCHEMA "{fixture.schema}" TO "{reader}"; GRANT SELECT ON "{fixture.schema}".registry_approved_record TO "{reader}"'
    )
    # Custody also resolves installed Tiger parent names when inputs are private copies.
    if await connection.fetchval("SELECT to_regnamespace('tiger') IS NOT NULL"):
        await connection.execute(f'GRANT USAGE ON SCHEMA tiger TO "{reader}"')
    proof = await provider_inputs.prepare_em_provider_inputs(connection, inputs, runtime_roles=(reader,))
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{address_schema}".entity_address_unified')
    manifest = PinnedNetworkServingManifest(1, str(address_id), address_schema, 1, {}, 0, "b" * 64, oid)
    await connection.execute(f'ANALYZE "{address_schema}".entity_address_unified')
    return SimpleNamespace(
        inputs=inputs,
        selection=selection,
        rates=receipt,
        manifest=manifest,
        proof=proof,
        windows=[],
        completion_bytes=None,
        schemas=(held_schema, address_schema, receipt.schema_name),
    )


async def _read_page(session, held, ordinals, *, limit=2, offset=0):
    parameters = address_scope.scoped_address_parameters({"allowed_binding_ordinals": list(ordinals), "code_index": 3})
    pagination = SimpleNamespace(limit=limit, offset=offset, page=1)
    args_by_name = {"code": "99213", "view": "card", "order_by": "distance", "zip5": "60611", "zip_radius_miles": "25"}
    window = await distance._progressive_page_rows(
        session,
        held.selection,
        args_by_name,
        pagination,
        ("60611", None, None),
        3,
        page_sql=managed._page_sql(held.inputs, held.rates.rates_relation),
        extra_parameters=parameters,
    )
    rows, exact, boundary, total = window
    cards = await managed._complete_page(session, held.inputs, held.rates.rates_relation, rows, ordinals, 3)
    return cards, exact, boundary, total


async def _assert_pages(session, held):
    cards, exact, boundary, total = await _read_page(session, held, (17, 29))
    assert [card["npi"] for card in cards] == [102, 107, 103] and exact and boundary and total == 3
    assert cards[0]["distance_miles"] == cards[1]["distance_miles"]
    await _assert_prelimit(session, held)
    office_card = cards[0]
    assert (office_card["city"], office_card["zip5"]) == ("ALLOWED OFFICE B", "60611")
    assert 0.6 < office_card["distance_miles"] < 0.8
    assert (
        office_card["minimum_negotiated_rate"],
        office_card["maximum_negotiated_rate"],
        office_card["rate_count"],
    ) == (
        Decimal("100.125"),
        Decimal("110.5"),
        5,
    )
    overlap = cards[-1]
    assert (overlap["minimum_negotiated_rate"], overlap["maximum_negotiated_rate"], overlap["rate_count"]) == (
        80,
        120,
        10,
    )
    all_cards, *_ = await _read_page(session, held, (3, 17, 29, 41, 42))
    assert [card["npi"] for card in all_cards][:2] == [101, 102] and all_cards[1]["rate_count"] == 55
    empty = await _read_page(session, held, ())
    assert empty == ([], True, False, 0)
    offset = await _read_page(session, held, (17, 29), offset=2)
    assert [card["npi"] for card in offset[0]] == [103] and offset[3] == 3
    with pytest.raises(PTG2LocationScopeError):
        await _read_page(session, held, (41,))
    assert [probe["candidate_limit"] for probe in held.windows if probe["ordinals"] == [41]] == [
        128,
        256,
        512,
        1024,
        2048,
        4096,
        8192,
    ]
    for system in ("CPT", "HCPCS"):
        assert (
            distance._request_code_index(
                {"code": "99213", "code_system": system, "view": "card", "order_by": "distance", "zip5": "60611"}
            )
            == 3
        )


async def _assert_prelimit(session, held):
    parameters = address_scope.scoped_address_parameters(
        {
            "allowed_binding_ordinals": [17, 29],
            "code_index": 3,
            "zip5": "60611",
            "latitude": None,
            "longitude": None,
            "radius_meters": 25 * 1609.344,
            "candidate_limit": 1,
            "page_limit": 2,
            "offset": 0,
        }
    )
    result = await session.execute(text(managed._page_sql(held.inputs, held.rates.rates_relation)), parameters)
    rows = result.mappings().all()
    assert len(rows) == 1 and rows[0]["npi"] == 102 and rows[0]["city"] == "ALLOWED OFFICE B"
    assert rows[0]["candidate_count"] == rows[0]["unique_count"] == 1


async def _assert_native_bounds(session, held, owner):
    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    completion = managed._completion_sql(held.inputs, held.rates.rates_relation)
    bounds = await driver.fetchrow(
        "WITH completed AS MATERIALIZED ("
        + completion
        + ") SELECT count(*) AS rows,coalesce(sum(octet_length(to_jsonb(completed)::text)),0)::bigint AS bytes FROM completed",
        [108],
        [42],
        3,
    )
    assert bounds["rows"] == 1 and bounds["bytes"] > 8388608
    held.completion_bytes = bounds["bytes"]
    with pytest.raises(PlanPricingProjectionUnavailable, match="completion"):
        await managed._complete_page(
            session, held.inputs, held.rates.rates_relation, [{"npi": 108, "distance_miles": 0}], (42,), 3
        )
    assert not await driver.fetchval(
        "SELECT has_column_privilege(current_user,$1::regclass,'npi','INSERT')", held.inputs.relation("npi")
    )
    for statement, error_type in (
        (f"INSERT INTO {held.inputs.relation('npi')} (npi) VALUES(999)", asyncpg.ReadOnlySQLTransactionError),
        (f'SET LOCAL ROLE "{owner}"', asyncpg.InsufficientPrivilegeError),
    ):
        with pytest.raises(error_type):
            async with driver.transaction():
                await driver.execute(statement)
    proof = await provider_inputs.prepare_em_provider_inputs(
        driver, held.inputs, runtime_roles=(await driver.fetchval("SELECT current_user"),)
    )
    assert proof == held.proof


async def _explain(session, held):
    parameters = address_scope.scoped_address_parameters(
        {
            "allowed_binding_ordinals": [17, 29],
            "code_index": 3,
            "zip5": "60611",
            "latitude": None,
            "longitude": None,
            "radius_meters": 25 * 1609.344,
            "candidate_limit": 128,
            "page_limit": 3,
            "offset": 0,
        }
    )
    result = await session.execute(
        text("EXPLAIN (FORMAT JSON) " + managed._page_sql(held.inputs, held.rates.rates_relation)), parameters
    )
    return result.scalar_one()


def _observe_windows(held):
    def observe(_connection, _cursor, _statement, _parameters, context, _executemany):
        for parameters in context.compiled_parameters or ():
            if "candidate_limit" in parameters:
                held.windows.append(
                    {
                        "ordinals": parameters["allowed_binding_ordinals"],
                        "candidate_limit": parameters["candidate_limit"],
                    }
                )

    return observe


async def _bind_native_pricing_read(session, held, control_schema, reader):
    """Bind actual prepared receipts to the original Reader before guarded native completion."""
    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    assert await provider_inputs.prepare_em_provider_inputs(driver, held.inputs, runtime_roles=(reader,)) == held.proof
    assert (
        await attribution._verify(driver, held.rates, attribution._identity(held.selection)[0], api_read=True)
        == held.rates
    )
    approved_oid = await driver.fetchval(
        "SELECT to_regclass($1)::oid::bigint", f'"{control_schema}".registry_approved_record'
    )
    relations = tuple(
        CompactAddressRelation(relation.schema_name, relation.table_name, relation.relation_oid)
        for relation in held.inputs.relations
    )
    await install_network_read_inventory(
        session,
        (
            *relations,
            CompactAddressRelation(held.rates.schema_name, "rates", held.rates.table_oid),
            CompactAddressRelation(
                held.manifest.schema_name, "entity_address_unified", held.manifest.address_table_oid
            ),
            CompactAddressRelation(control_schema, "registry_approved_record", approved_oid),
        ),
    )


async def test_managed_em_native_read(migrated_v3_database, monkeypatch):
    """Execute the unchanged managed SQL under real protected custody and Reader RR."""
    fixture = migrated_v3_database
    connection = await asyncpg.connect(os.environ[POSTGRES_DSN_ENV])
    owner, reader = "em_owner_" + uuid4().hex, "em_reader_" + uuid4().hex
    password = uuid4().hex
    held = reader_engine = None
    try:
        await connection.execute(f'CREATE ROLE "{owner}" NOLOGIN; CREATE ROLE "{reader}" LOGIN PASSWORD \'{password}\'')
        database = await connection.fetchval("SELECT current_database()")
        await connection.execute(f'GRANT CREATE ON DATABASE "{database}" TO "{owner}"')
        async with connection.transaction(isolation="repeatable_read"):
            held = await _prepare(connection, fixture, owner, reader)
        url = make_url(_sqlalchemy_async_dsn(os.environ[POSTGRES_DSN_ENV])).set(username=reader, password=password)
        reader_engine = create_async_engine(url, isolation_level="REPEATABLE READ")
        event.listen(reader_engine.sync_engine, "before_cursor_execute", _observe_windows(held))
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.schema)
        async with async_sessionmaker(reader_engine)() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            role = await driver.fetchrow(
                "SELECT rolname,rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user"
            )
            assert role["rolname"] == reader and not role["rolsuper"] and not role["rolbypassrls"]
            await _bind_native_pricing_read(session, held, fixture.schema, reader)
            with address_scope.network_address_read_scope(
                held.manifest, None, access_scope_sha256="c" * 64, excluded_network_ids=(7,)
            ):
                await _assert_pages(session, held)
                await _assert_native_bounds(session, held, owner)
                plan = await _explain(session, held)
        if evidence := os.getenv("MANAGED_EM_NATIVE_EVIDENCE"):
            Path(evidence).write_text(
                json.dumps(
                    {
                        "plan": plan,
                        "ordinary_reader": True,
                        "repeatable_read": True,
                        "provider_custody_real": True,
                        "attribution_publication_real": True,
                        "rates_rows": held.rates.row_count,
                        "offices": 8208,
                        "native_window_observations": held.windows,
                        "oversized_completion_bytes": held.completion_bytes,
                        "full_manifest_resolution": False,
                        "http": False,
                    },
                    indent=2,
                )
                + "\n"
            )
    finally:
        if reader_engine is not None:
            await reader_engine.dispose()
        if held is not None:
            for schema in held.schemas:
                await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for role in (reader, owner):
            await connection.execute(f'DROP OWNED BY "{role}"; DROP ROLE IF EXISTS "{role}"')
        await connection.close()
