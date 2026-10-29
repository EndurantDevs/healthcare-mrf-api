# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admitted source ordinals precede real card and statewide PostgreSQL work."""

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import orjson
import pytest
from sqlalchemy import event, text

from api import plan_pricing_projection_aggregate_read as aggregate_read
from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_read as projection_read
from api import plan_pricing_state_scan as scan
from api import provider_service_source_policy as policy
from api import ptg2_serving as serving
from api.custom_import_provider_detail import parse_managed_provider_policy
from api.provider_detail_network_read import managed_provider_network_read_scope
from db.connection import current_session
from tests.managed_billing_sql_postgres_support import billing_office_inputs as billing_office_inputs
from tests.ptg2_factorized_card_support import card_pagination, provider_cell_row
from tests.test_network_catalog_routes import v2_envelope
from tests.test_plan_pricing_projection_postgres import _create_import_run_stub, _migration_statements
from tests.test_plan_pricing_projection_v3_postgres import _factorized_migration_statements
from tests.test_provider_service_source_policy_postgres import (
    _reader_identity,
    _selection,
)
from tests.test_provider_service_source_policy_postgres import (
    cms_resource_template as cms_resource_template,
)
from tests.test_provider_service_source_policy_postgres import (
    profile_transport as profile_transport,
)
from tests.test_provider_service_source_policy_postgres import (
    published_profile_source as published_profile_source,
)
from tests.test_provider_service_source_policy_postgres import (
    source_admission as source_admission,
)
from tests.test_ptg2_factorized_card_postgres import _insert_factorized_card_fixture

pytestmark = pytest.mark.asyncio
PROJECTION_ID = "d" * 64


async def _migrate_projection(held, schema, monkeypatch):
    await held.fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
    await _create_import_run_stub(held.fixture.connection, f'"{schema}"')
    migration = Path(__file__).resolve().parents[1] / "alembic/versions/20260903160000_plan_pricing_state_scan.py"
    with monkeypatch.context() as migration_patch:
        statements = _migration_statements(migration_patch, schema)
        statements += _factorized_migration_statements(migration_patch, schema)
        statements += _factorized_migration_statements(migration_patch, schema, migration_path=migration)
    for statement in statements:
        await held.fixture.connection.execute(statement)


async def _seed_card_rows(connection, schema):
    await _insert_factorized_card_fixture(connection, schema, PROJECTION_ID)
    await connection.execute(
        text(
            f'UPDATE "{schema}".plan_pricing_provider_cell '
            "SET taxonomy_codes=ARRAY['207X00000X']::text[] WHERE npi=103"
        )
    )
    for table in ("plan_pricing_provider_membership", "plan_pricing_rate_profile"):
        await connection.execute(
            text(
                f'UPDATE "{schema}".{table} SET binding_ordinal=CASE binding_ordinal '
                "WHEN 0 THEN 3 WHEN 1 THEN 10 WHEN 2 THEN 17 END"
            )
        )
    await connection.execute(
        text(
            f'UPDATE "{schema}".plan_pricing_rate_profile SET membership_count=2,'
            "minimum_negotiated_rate=1,maximum_negotiated_rate=1000,rate_count=100,"
            "negotiated_rates=ARRAY[1,1000]::numeric[],rate_multiplicities=ARRAY[50,50]::bigint[] "
            "WHERE binding_ordinal=10"
        )
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_pricing_provider_membership '
            "(projection_id,binding_ordinal,provider_set_key,npi) VALUES(:projection,10,20,100)"
        ),
        {"projection": PROJECTION_ID},
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_pricing_provider_membership '
            "(projection_id,binding_ordinal,provider_set_key,npi) "
            "SELECT :projection,10,provider_set,101 FROM generate_series(1000,3050) provider_set"
        ),
        {"projection": PROJECTION_ID},
    )
    cell_by_field = provider_cell_row(100, "10001") | {"projection_id": PROJECTION_ID}
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_pricing_provider_cell '
            "(projection_id,geo_cell,npi,entity_type_code,taxonomy_codes,fragment) "
            "VALUES(:projection_id,:geo_cell,:npi,:entity_type_code,:taxonomy_codes,:fragment)"
        ),
        cell_by_field,
    )


async def _seed_state_rows(connection, schema):
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_pricing_provider_state '
            "(projection_id,state,npi,provider_fragment) SELECT DISTINCT ON (npi) projection_id,'IL',npi,fragment "
            f'FROM "{schema}".plan_pricing_provider_cell ORDER BY npi,geo_cell'
        )
    )
    await connection.execute(
        text(
            f'INSERT INTO "{schema}".plan_pricing_rate_occurrence '
            "(projection_id,code_system,code,binding_ordinal,occurrence_ordinal,provider_set_key,provider_set_ref,"
            "price_key,price_set_ref,rate_pack_ref,source_artifact_key,provider_count,group_fragment,occurrence_multiplicity) "
            "VALUES(:projection,'CPT','27447',:ordinal,0,:provider_set,:provider_ref,9,:price_ref,:pack_ref,11,2,"
            '\'{"reported_code_system":"CPT","reported_code":"27447"}\'::jsonb,:multiplicity)'
        ),
        [
            {
                "projection": PROJECTION_ID,
                "ordinal": ordinal,
                "provider_set": provider_set,
                "provider_ref": "1" * 32,
                "price_ref": "2" * 32,
                "pack_ref": "3" * 32,
                "multiplicity": multiplicity,
            }
            for ordinal, provider_set, multiplicity in ((3, 10, 3), (10, 20, 1000), (17, 30, 1))
        ],
    )


@pytest.fixture
async def pricing_policy(source_admission, monkeypatch):
    admission = source_admission
    held = admission.held
    schema = "pricing_source_" + uuid4().hex
    try:
        await _migrate_projection(held, schema, monkeypatch)
        async with held.fixture.engine.begin() as connection:
            await _seed_card_rows(connection, schema)
            await _seed_state_rows(connection, schema)
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{held.role}"'))
            await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{held.role}"'))
        monkeypatch.setattr(projection_contract, "SCHEMA", schema)
        selection = _selection([admission.reviews_by_name[name] for name in ("allowed", "excluded", "public")])
        yield SimpleNamespace(admission=admission, held=held, schema=schema, selection=selection)
    finally:
        await held.fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@asynccontextmanager
async def _admitted_reader(fixture):
    held, admission = fixture.held, fixture.admission
    document = v2_envelope({}, excluded_network_ids=[9002])
    _, exclusions, digest, organization = parse_managed_provider_policy(document)
    original = fixture.selection.bindings
    queries = []
    held.writer_statements.clear()
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        driver.add_query_logger(queries.append)
        try:
            pid = await _reader_identity(driver, held)
            transaction = await driver.fetchval(
                "SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND locktype='virtualxid' AND granted"
            )
            async with managed_provider_network_read_scope(
                session,
                selector=None,
                network_generation=admission.manifest.generation_id,
                excluded_network_ids=exclusions,
                access_scope_sha256=digest,
                organization_policy=organization,
            ) as manifest:
                assert manifest == admission.manifest and current_session() is session
                assert session.info["api_reader_verified"]
                admitted = await policy.admitted_plan_release_bindings(session, fixture.selection)
                assert tuple(binding.binding_ordinal for binding in admitted) == (3, 17)
                assert admitted[0] is original[0] and admitted[1] is original[2]
                assert not await driver.fetchval(
                    "SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE')",
                    fixture.schema + ".plan_pricing_provider_membership",
                )
                yield session, admitted, queries
                assert await driver.fetchval("SELECT pg_backend_pid()") == pid
                assert (
                    await driver.fetchval(
                        "SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
                        "AND locktype='virtualxid' AND granted"
                    )
                    == transaction
                )
                assert await _reader_identity(driver, held) == pid
                assert fixture.selection.bindings is original
        finally:
            await asyncio.sleep(0)
            driver.remove_query_logger(queries.append)
    assert not any("binding_matches AS MATERIALIZED" in query.query for query in queries)
    assert sum("to_jsonb(candidate)::text AS candidate_json" in query.query for query in queries) == 2
    assert sum("validation_json::text AS validation_json" in query.query for query in queries) == 1
    assert sum(query.query.startswith("WITH source_rows AS MATERIALIZED") for query in queries) == 3
    assert not held.writer_statements and not held.database.has_reader_session()
    assert len(set(held.transactions)) == 1


async def test_native_card_admission_precedes_candidates_and_completion(pricing_policy):
    fixture = pricing_policy
    async with _admitted_reader(fixture) as (session, admitted, queries):
        ordinals = tuple(binding.binding_ordinal for binding in admitted)
        args_by_name = {"code_system": "CPT", "code": "27447"}
        prefix = await serving._factorized_card_candidates(
            session, PROJECTION_ID, ("CPT", "27447"), ["10002", "10001"], args_by_name, 1, binding_ordinals=ordinals
        )
        assert prefix.minimum_rate_by_npi == {103: Decimal(5)}
        assert prefix.total_lower_bound == 3 and prefix.total_is_exact
        candidates = await serving._factorized_card_candidates(
            session,
            PROJECTION_ID,
            ("CPT", "27447"),
            ["10002", "10001"],
            args_by_name,
            3,
            binding_ordinals=ordinals,
        )
        assert candidates.minimum_rate_by_npi == {101: Decimal(10), 102: Decimal(10), 103: Decimal(5)}
        assert candidates.total_lower_bound == 3 and candidates.total_is_exact
        selected_npis = tuple(candidates.minimum_rate_by_npi)
        cards = await serving._factorized_card_complete_selected_npis(
            session,
            PROJECTION_ID,
            ("CPT", "27447"),
            ["10002", "10001"],
            args_by_name,
            selected_npis,
            candidates.minimum_rate_by_npi,
            binding_ordinals=ordinals,
        )
        assert [card["npi"] for card in cards] == [103, 101, 102]
        shared = next(card for card in cards if card["npi"] == 101)
        assert shared["minimum_negotiated_rate"] == 10 and shared["maximum_negotiated_rate"] == 20
        assert shared["rate_count"] == 3 and shared["zip5"] == "10002"
        page = serving._factorized_card_response(
            fixture.selection,
            args_by_name,
            card_pagination(limit=1, offset=1),
            cards,
            candidates,
            has_geo_cells=True,
            contributing_bindings=admitted,
        )
        assert page["pagination"]["total"] == 3 and page["items"][0]["npi"] == 101
        assert page["resolved_snapshot_ids"] == sorted(binding.snapshot_id for binding in admitted)
        assert "snapshot_excluded" not in json.dumps(page)
        empty = await serving._factorized_card_candidates(
            session, PROJECTION_ID, ("CPT", "27447"), ["10001"], args_by_name, 1, binding_ordinals=()
        )
        assert empty.minimum_rate_by_npi == {} and empty.total_lower_bound == 0
        assert all(query.exception is None for query in queries)


async def test_native_state_admission_precedes_provider_and_work_limits(pricing_policy):
    fixture = pricing_policy
    async with _admitted_reader(fixture) as (session, admitted, queries):
        ordinals = tuple(binding.binding_ordinal for binding in admitted)
        selected, fragments, has_more = await scan._read_state_npis(
            session, PROJECTION_ID, "IL", 0, 1, binding_ordinals=ordinals
        )
        assert selected == (101,) and has_more and set(fragments) == {101}
        assert json.loads(fragments[101])["npi"] == 101
        occurrences = scan._validated_occurrences(
            await scan._read_projected_occurrences(
                session, PROJECTION_ID, "CPT", "27447", selected, 1, binding_ordinals=ordinals
            )
        )
        assert [
            (occurrence["npi"], occurrence["binding_ordinal"], occurrence["provider_set_key"])
            for occurrence in occurrences
        ] == [(101, 3, 10)]
        assert occurrences[0]["occurrence_multiplicity"] == 3 and occurrences[0]["provider_set_ref"] == "1" * 32
        continuation, _, has_more = await scan._read_state_npis(
            session, PROJECTION_ID, "IL", 101, 2, binding_ordinals=ordinals
        )
        assert continuation == (102, 103) and not has_more
        public_rows = scan._validated_occurrences(
            await scan._read_projected_occurrences(
                session, PROJECTION_ID, "CPT", "27447", (103,), 1, binding_ordinals=ordinals
            )
        )
        assert [(occurrence["npi"], occurrence["binding_ordinal"]) for occurrence in public_rows] == [(103, 17)]
        unscoped, _, _ = await scan._read_state_npis(session, PROJECTION_ID, "IL", 0, 1)
        assert unscoped == (100,)
        with pytest.raises(scan.PlanPricingStateScanBudgetExceeded):
            scan._validated_occurrences(
                await scan._read_projected_occurrences(session, PROJECTION_ID, "CPT", "27447", (101,), 1)
            )
        denied, _, has_more = await scan._read_state_npis(session, PROJECTION_ID, "IL", 0, 1, binding_ordinals=())
        assert denied == () and not has_more
        assert all(query.exception is None for query in queries)


async def _seed_aggregate_rates(fixture):
    profiles = (
        (3, [Decimal("10.25"), Decimal("20.75")], [2, 1]),
        (17, [Decimal(5), Decimal(15), Decimal(25)], [1, 1, 1]),
    )
    async with fixture.held.fixture.engine.begin() as connection:
        for ordinal, rates, multiplicities in profiles:
            await connection.execute(
                text(
                    f'UPDATE "{fixture.schema}".plan_pricing_rate_profile '
                    "SET negotiated_rates=CAST(:rates AS numeric[]),"
                    "rate_multiplicities=CAST(:multiplicities AS bigint[]),"
                    "minimum_negotiated_rate=:minimum,maximum_negotiated_rate=:maximum,rate_count=:count "
                    "WHERE projection_id=:projection AND binding_ordinal=:ordinal"
                ),
                {
                    "rates": rates,
                    "multiplicities": multiplicities,
                    "minimum": rates[0],
                    "maximum": rates[-1],
                    "count": sum(multiplicities),
                    "projection": PROJECTION_ID,
                    "ordinal": ordinal,
                },
            )


def _aggregate_selection(fixture):
    selection = replace(
        fixture.selection,
        pricing_projection_id=PROJECTION_ID,
        pricing_projection_contract=projection_contract.PROJECTION_CONTRACT,
    )
    assert selection.bindings is fixture.selection.bindings
    return selection


def _aggregate_arguments(code_system="CPT"):
    return {
        "code_system": code_system,
        "code": "27447",
        "include_providers": False,
        "view": "card",
        "zip5": "10001",
        "zip_radius_miles": 0,
    }


def _aggregate_query_count(queries):
    return sum("WITH profile_window AS MATERIALIZED" in query.query for query in queries)


@contextmanager
def _observe_aggregate_sql(fixture, queries):
    engine = fixture.held.database._reader_database.engine.sync_engine

    def record_aggregate(connection, cursor, statement, parameters, context, executemany):
        if "WITH profile_window AS MATERIALIZED" in statement:
            queries.append(SimpleNamespace(query=statement, exception=None))

    event.listen(engine, "after_cursor_execute", record_aggregate)
    try:
        yield
    finally:
        event.remove(engine, "after_cursor_execute", record_aggregate)


async def _observed_aggregate_page(session, request, queries, ordinals, *, order="asc", offset=0, cells=None):
    query_count = _aggregate_query_count(queries)
    fragments, total = await aggregate_read.read_aggregate_pack_page(
        session,
        request,
        cells or ["10002", "10001"],
        {"order": order},
        card_pagination(limit=1, offset=offset),
        binding_ordinals=ordinals,
    )
    await asyncio.sleep(0)
    assert _aggregate_query_count(queries) == query_count + 1
    return json.loads(orjson.dumps(fragments), parse_float=Decimal), total


async def _assert_aggregate_precision(session, request, ordinals, queries):
    query_count = _aggregate_query_count(queries)
    aggregates = await aggregate_read._partial_aggregate_records(session, request, ["10001", "10002"], ordinals)
    await asyncio.sleep(0)
    assert _aggregate_query_count(queries) == query_count + 1
    assert [
        (
            aggregate.zip5,
            aggregate.provider_count,
            aggregate.rate_count,
            aggregate.minimum_negotiated_rate,
            aggregate.median_negotiated_rate,
            aggregate.maximum_negotiated_rate,
        )
        for aggregate in aggregates
    ] == [
        ("10001", 3, 6, Decimal(5), Decimal("12.625"), Decimal(25)),
        ("10002", 1, 3, Decimal("10.25"), Decimal("10.25"), Decimal("20.75")),
    ]
    for order, offset, zip5 in [("asc", 0, "10001"), ("desc", 0, "10002"), ("asc", 1, "10002")]:
        rendered, total = await _observed_aggregate_page(
            session, request, queries, ordinals, order=order, offset=offset
        )
        assert total == 2 and rendered[0]["geo_cell"] == zip5
        if zip5 == "10001":
            assert rendered[0] == {
                "geo_cell": zip5,
                "provider_count": 3,
                "rate_count": 6,
                "minimum_negotiated_rate": 5,
                "median_negotiated_rate": Decimal("12.625"),
                "maximum_negotiated_rate": 25,
            }
    for offset, zip5 in [(0, "10001"), (1, "10002")]:
        rendered, total = await _observed_aggregate_page(session, request, queries, (3,), order="desc", offset=offset)
        assert total == 2 and rendered[0]["geo_cell"] == zip5
        assert rendered[0]["minimum_negotiated_rate"] == Decimal("10.25")
        assert rendered[0]["median_negotiated_rate"] == Decimal("10.25")
        assert rendered[0]["maximum_negotiated_rate"] == Decimal("20.75")
    rendered, total = await _observed_aggregate_page(session, request, queries, ordinals, offset=5)
    assert rendered == [] and total == 2


async def _assert_aggregate_bounds(session, request, ordinals, queries, monkeypatch):
    for constant, limit in [
        ("_MAX_PARTIAL_PROFILES", 2),
        ("_MAX_PARTIAL_MEMBERSHIPS", 3),
        ("_MAX_PARTIAL_RATE_VALUES", 5),
    ]:
        with monkeypatch.context() as budget_patch:
            budget_patch.setattr(aggregate_read, constant, limit)
            rendered, total = await _observed_aggregate_page(session, request, queries, ordinals, cells=["10001"])
            assert total == 1 and rendered[0]["rate_count"] == 6
            query_count = _aggregate_query_count(queries)
            with pytest.raises(projection_contract.PlanPricingProjectionUnsupported, match="native work bound"):
                await aggregate_read.read_aggregate_pack_page(
                    session,
                    request,
                    ["10001"],
                    {},
                    card_pagination(),
                    binding_ordinals=(3, 10, 17),
                )
            await asyncio.sleep(0)
            assert _aggregate_query_count(queries) == query_count + 1
    with monkeypatch.context() as byte_patch:
        byte_patch.setattr(aggregate_read, "MAX_AGGREGATE_REQUEST_STORED_BYTES", 1)
        query_count = _aggregate_query_count(queries)
        with pytest.raises(projection_contract.PlanPricingProjectionUnsupported, match="byte bound"):
            await aggregate_read.read_aggregate_pack_page(
                session,
                request,
                ["10001"],
                {},
                card_pagination(),
                binding_ordinals=ordinals,
            )
        await asyncio.sleep(0)
        assert _aggregate_query_count(queries) == query_count + 1


async def _assert_aggregate_projection(session, selection, admitted, queries):
    pages_by_system = {}
    for code_system in ("CPT", "HCPCS"):
        args_by_name = _aggregate_arguments(code_system)
        request = projection_read._validated_projection_request(selection, args_by_name)
        assert (request.code_system, request.code) == ("CPT", "27447")
        query_count = _aggregate_query_count(queries)
        page_by_field = await projection_read._projection_page_response(
            session,
            request,
            args_by_name,
            card_pagination(limit=1),
            admitted,
            selection,
        )
        await asyncio.sleep(0)
        assert _aggregate_query_count(queries) == query_count + 1
        assert page_by_field["query"]["code_system"] == code_system and page_by_field["query"]["code"] == "27447"
        assert page_by_field["pagination"] == {
            "total": 1,
            "total_is_exact": True,
            "total_lower_bound": 1,
            "limit": 1,
            "offset": 0,
            "page": 1,
            "has_more": False,
        }
        pages_by_system[code_system] = json.loads(orjson.dumps(page_by_field["items"]), parse_float=Decimal)
        assert "snapshot_excluded" not in orjson.dumps(page_by_field).decode()
    assert pages_by_system["CPT"] == pages_by_system["HCPCS"]
    assert pages_by_system["HCPCS"][0]["median_negotiated_rate"] == Decimal("12.625")
    query_count = len(queries)
    empty_fragments, total = await aggregate_read.read_aggregate_pack_page(
        session,
        request,
        ["10001"],
        {},
        card_pagination(),
        binding_ordinals=(),
    )
    empty_by_field = await projection_read._projection_page_response(
        session,
        request,
        args_by_name,
        card_pagination(),
        (),
        selection,
    )
    await asyncio.sleep(0)
    assert len(queries) == query_count and empty_fragments == [] and total == 0
    assert empty_by_field["items"] == [] and empty_by_field["pagination"]["total"] == 0
    assert empty_by_field["pagination"]["total_is_exact"] and empty_by_field["result_state"] == "no_matching_rates"
    assert "snapshot_excluded" not in orjson.dumps(empty_by_field).decode()


async def test_native_partial_aggregate_prices_and_paging(pricing_policy, monkeypatch):
    fixture = pricing_policy
    await _seed_aggregate_rates(fixture)
    selection = _aggregate_selection(fixture)
    request = projection_read._validated_projection_request(selection, _aggregate_arguments())
    async with _admitted_reader(fixture) as (session, admitted, queries):
        ordinals = tuple(binding.binding_ordinal for binding in admitted)
        with _observe_aggregate_sql(fixture, queries):
            await _assert_aggregate_precision(session, request, ordinals, queries)
            await _assert_aggregate_bounds(session, request, ordinals, queries, monkeypatch)
            await _assert_aggregate_projection(session, selection, admitted, queries)
        assert all(query.exception is None for query in queries)


from uuid import UUID

from sqlalchemy import MetaData

from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


async def _em_provider_native_heaps(operator, schema, roles, created_schema_oids_by_name):
    from sqlalchemy.dialects.postgresql import dialect
    from sqlalchemy.schema import CreateTable

    from process import network_em_provider_inputs as provider_inputs

    await operator.execute(f'SET ROLE "{roles["owner"]}"')
    try:
        for namespace in (schema, "tiger"):
            await operator.execute(f'CREATE SCHEMA "{namespace}"')
            created_schema_oids_by_name[namespace] = await operator.fetchval(
                "SELECT to_regnamespace($1)::oid::bigint", namespace
            )
        await operator.execute("CREATE TABLE tiger.zcta5(zcta5ce varchar(5),the_geom geometry(MultiPolygon,4269))")
        await operator.execute("CREATE TABLE tiger.zip_state(zip varchar(5),stusps varchar(2))")
        for role, model in provider_inputs._MODELS.items():
            table = model.__table__.to_metadata(MetaData(), schema=schema, name=role)
            await operator.execute(str(CreateTable(table).compile(dialect=dialect())))
        await operator.execute(f'CREATE TABLE "{schema}".held_zcta5() INHERITS(tiger.zcta5)')
        await operator.execute(f'CREATE TABLE "{schema}".held_zip_state() INHERITS(tiger.zip_state)')
        await operator.execute(f'INSERT INTO "{schema}".npi(npi,entity_type_code) VALUES(1000000001,1)')
        await operator.execute(f"INSERT INTO \"{schema}\".nucc_taxonomy(code,int_code) VALUES('207Q00000X',1)")
        await operator.execute(f"INSERT INTO \"{schema}\".geo_zip_lookup(zip_code,state) VALUES('10001','NY')")
        await operator.execute(f'GRANT USAGE ON SCHEMA "{schema}",tiger TO "{roles["reader"]}"')
        await operator.execute(f'GRANT USAGE ON SCHEMA tiger TO "{roles["operator"]}"')
        await operator.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles["reader"]}"')
    finally:
        await operator.execute("RESET ROLE")
    relations = []
    for role in provider_inputs._ROLES:
        table = "held_" + role.split(".")[1] if role.startswith("tiger.") else role
        physical = await operator.fetchrow(
            "SELECT c.oid::bigint,pg_relation_filenode(c.oid)::bigint AS filenode FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
            schema,
            table,
        )
        relations.append(
            provider_inputs.EMProviderInputRelation(
                role, schema, table, "retained-generation-1", physical["oid"], physical["filenode"]
            )
        )
    return provider_inputs.EMProviderInputs(tuple(relations))


async def _em_provider_create_roles(admin, control, roles_by_kind, password_by_kind, created_roles):
    for kind, name in roles_by_kind.items():
        login = "NOLOGIN" if kind == "owner" else "LOGIN PASSWORD '" + password_by_kind[kind] + "'"
        await admin.execute(
            f'CREATE ROLE "{name}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        created_roles.append(name)
    dbname = await admin.fetchval("SELECT quote_ident(current_database())")
    await admin.execute(
        f'GRANT CREATE ON DATABASE {dbname} TO "{roles_by_kind["operator"]}","{roles_by_kind["owner"]}"'
    )
    await admin.execute(
        f'GRANT "{roles_by_kind["owner"]}" TO "{roles_by_kind["operator"]}" WITH INHERIT FALSE, SET TRUE'
    )
    await admin.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{roles_by_kind["operator"]}"')
    await admin.execute(
        f'GRANT SELECT,INSERT,UPDATE ON "{control}".network_membership_candidate TO "{roles_by_kind["operator"]}"'
    )


async def _em_provider_configure_reader(database, operator, roles_by_kind, password_by_kind, schema, monkeypatch):
    settings_by_name = {
        "READER_USER": roles_by_kind["reader"],
        "READER_PASSWORD": password_by_kind["reader"],
        "READER_POOL_MIN_SIZE": "1",
        "READER_POOL_MAX_SIZE": "1",
        "SCHEMA": schema,
    }
    for name, setting in settings_by_name.items():
        monkeypatch.setenv("HLTHPRT_DB_" + name, setting)
    database._database_override = await operator.fetchval("SELECT current_database()")


@asynccontextmanager
async def _em_provider_native_context(serving_schema, monkeypatch):
    import os
    import secrets

    import asyncpg

    from db.connection import Database
    from process.network_membership_copy import MembershipCopyTarget

    admin, control, _ = serving_schema
    suffix = uuid4().hex
    schema = "em_inputs_" + suffix
    roles_by_kind = {kind: "em_" + kind + "_" + suffix for kind in ("owner", "operator", "reader")}
    password_by_kind = {kind: secrets.token_urlsafe(24) for kind in ("operator", "reader")}
    candidate = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate), "network_candidate_" + candidate.hex
    )
    operator = reader = None
    database = Database()
    created_roles, created_schema_oids_by_name = [], {}
    try:
        await _em_provider_create_roles(admin, control, roles_by_kind, password_by_kind, created_roles)
        dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
        operator = await asyncpg.connect(dsn, user=roles_by_kind["operator"], password=password_by_kind["operator"])
        held = await _em_provider_native_heaps(operator, schema, roles_by_kind, created_schema_oids_by_name)
        reader = await asyncpg.connect(dsn, user=roles_by_kind["reader"], password=password_by_kind["reader"])
        await _em_provider_configure_reader(database, operator, roles_by_kind, password_by_kind, schema, monkeypatch)
        yield SimpleNamespace(
            admin=admin,
            operator=operator,
            reader=reader,
            database=database,
            roles=roles_by_kind,
            schema=schema,
            control=control,
            target=copy_target,
            inputs=held,
        )
    finally:
        if database._reader_database is not None:
            await database._reader_database.disconnect()
        for connection in (reader, operator):
            if connection is not None:
                await connection.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        assert await admin.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None
        for namespace, oid in created_schema_oids_by_name.items():
            if await admin.fetchval("SELECT to_regnamespace($1)::oid::bigint", namespace) == oid:
                await admin.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
                assert await admin.fetchval("SELECT to_regnamespace($1)", namespace) is None
        for name in reversed(created_roles):
            await admin.execute(f'DROP OWNED BY "{name}"')
            await admin.execute(f'DROP ROLE "{name}"')
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


async def _em_provider_reader_receipt(fixture):
    from process.network_em_provider_inputs import prepare_em_provider_inputs

    async with fixture.database.reader_session(independent=True) as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        identity = await driver.fetchrow(
            "SELECT session_user,current_user,current_setting('transaction_isolation') AS isolation,"
            "current_setting('transaction_read_only') AS readonly,rolsuper,rolcanlogin "
            "FROM pg_roles WHERE rolname=current_user"
        )
        assert (identity["session_user"], identity["current_user"], identity["isolation"], identity["readonly"]) == (
            fixture.roles["reader"],
            fixture.roles["reader"],
            "repeatable read",
            "on",
        )
        assert identity["rolsuper"] is False and identity["rolcanlogin"] is True
        assert session.info["api_reader_verified"] is True
        return await prepare_em_provider_inputs(driver, fixture.inputs, runtime_roles=(fixture.roles["reader"],))


async def _em_provider_native_record_checks(fixture, receipt):
    from process.network_em_provider_inputs import record_em_provider_inputs
    from process.network_membership_candidate_lifecycle import create_network_candidate

    operator = fixture.operator
    async with operator.transaction(isolation="repeatable_read"):
        await create_network_candidate(
            operator,
            fixture.target,
            source_generations={"em_provider_inputs": receipt["receipt_sha256"]},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=0,
            control_schema=fixture.control,
        )
        assert await record_em_provider_inputs(operator, fixture.target, receipt, fixture.control) == receipt
    query = f'SELECT source_generations::text,validation_json::text,state FROM "{fixture.control}".network_membership_candidate WHERE candidate_id=$1'
    before = await operator.fetchrow(query, UUID(fixture.target.candidate_id))
    async with operator.transaction(isolation="repeatable_read"):
        assert await record_em_provider_inputs(operator, fixture.target, receipt, fixture.control) == receipt
    assert await operator.fetchrow(query, UUID(fixture.target.candidate_id)) == before
    for damage in ("scope", "digest", "collision", "closed"):
        with pytest.raises(ValueError):
            async with operator.transaction(isolation="repeatable_read"):
                requested = fixture.target
                if damage == "scope":
                    requested = replace(requested, dataset_id=str(uuid4()))
                if damage == "digest":
                    await operator.execute(
                        f"UPDATE \"{fixture.control}\".network_membership_candidate SET source_generations='{{}}'::jsonb WHERE candidate_id=$1",
                        UUID(requested.candidate_id),
                    )
                if damage == "collision":
                    await operator.execute(
                        f'UPDATE "{fixture.control}".network_membership_candidate SET validation_json=\'{{"em_provider_inputs":{{}}}}\'::jsonb WHERE candidate_id=$1',
                        UUID(requested.candidate_id),
                    )
                if damage == "closed":
                    await operator.execute(
                        f"UPDATE \"{fixture.control}\".network_membership_candidate SET state='sealed' WHERE candidate_id=$1",
                        UUID(requested.candidate_id),
                    )
                await record_em_provider_inputs(operator, requested, receipt, fixture.control)
        assert await operator.fetchrow(query, UUID(fixture.target.candidate_id)) == before


async def _em_provider_native_denials(fixture):
    import asyncpg

    reader = fixture.reader
    table = fixture.inputs.relation("npi")
    assert await reader.fetchval(f"SELECT count(*) FROM {table}") == 1
    for statement in (
        f"INSERT INTO {table}(npi) VALUES(1000000002)",
        f"UPDATE {table} SET entity_type_code=2",
        f"DELETE FROM {table}",
        f"TRUNCATE {table}",
        f'CREATE TABLE "{fixture.schema}".extra(id integer)',
        f'SET ROLE "{fixture.roles["owner"]}"',
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as denied:
            await reader.execute(statement)
        assert denied.value.sqlstate == "42501"


async def _em_provider_custody_mutations(fixture, receipt):
    table = fixture.inputs.relation("npi")
    mutations = (
        (
            f'GRANT UPDATE ON {table} TO "{fixture.roles["reader"]}"',
            f'REVOKE UPDATE ON {table} FROM "{fixture.roles["reader"]}"',
        ),
        (
            f'GRANT CREATE ON SCHEMA "{fixture.schema}" TO "{fixture.roles["reader"]}"',
            f'REVOKE CREATE ON SCHEMA "{fixture.schema}" FROM "{fixture.roles["reader"]}"',
        ),
        (
            f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["owner"]}" IN SCHEMA "{fixture.schema}" GRANT UPDATE ON TABLES TO PUBLIC',
            f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["owner"]}" IN SCHEMA "{fixture.schema}" REVOKE UPDATE ON TABLES FROM PUBLIC',
        ),
        (f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY", f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY"),
        (
            f"ALTER TABLE {fixture.inputs.relation('nucc_taxonomy')} ALTER COLUMN definition TYPE varchar",
            f"ALTER TABLE {fixture.inputs.relation('nucc_taxonomy')} ALTER COLUMN definition TYPE text",
        ),
    )
    for apply, undo in mutations:
        await fixture.admin.execute(apply)
        try:
            with pytest.raises(ValueError):
                await _em_provider_reader_receipt(fixture)
        finally:
            await fixture.admin.execute(undo)
        assert await _em_provider_reader_receipt(fixture) == receipt


async def _em_provider_unique_key_refusals(fixture, receipt):
    from process.network_em_provider_inputs import prepare_em_provider_inputs

    for role in ("geo_zip_lookup", "nucc_taxonomy"):
        with pytest.raises(ValueError):
            async with fixture.operator.transaction(isolation="repeatable_read"):
                await fixture.operator.execute(f'SET LOCAL ROLE "{fixture.roles["owner"]}"')
                await fixture.operator.execute(
                    f"ALTER TABLE {fixture.inputs.relation(role)} DROP CONSTRAINT {role}_pkey"
                )
                await prepare_em_provider_inputs(
                    fixture.operator, fixture.inputs, runtime_roles=(fixture.roles["reader"],)
                )
        assert await _em_provider_reader_receipt(fixture) == receipt


async def test_native_em_provider_inputs_custody_and_candidate_receipts(serving_schema, monkeypatch, tmp_path):
    from process.network_em_provider_inputs import EMProviderInputs, prepare_em_provider_inputs

    async with _em_provider_native_context(serving_schema, monkeypatch) as fixture:
        identity = await fixture.operator.fetchrow(
            "SELECT session_user,current_user,rolsuper,rolcanlogin FROM pg_roles WHERE rolname=current_user"
        )
        assert identity["session_user"] == identity["current_user"] == fixture.roles["operator"]
        assert identity["rolsuper"] is False and identity["rolcanlogin"] is True
        async with fixture.operator.transaction(isolation="repeatable_read"):
            receipt = await prepare_em_provider_inputs(
                fixture.operator, fixture.inputs, runtime_roles=(fixture.roles["reader"],)
            )
        assert len(receipt["relations"]) == 9
        assert await _em_provider_reader_receipt(fixture) == receipt
        await _em_provider_native_denials(fixture)
        await _em_provider_native_record_checks(fixture, receipt)
        await _em_provider_custody_mutations(fixture, receipt)
        await _em_provider_unique_key_refusals(fixture, receipt)
        changed_relations = list(fixture.inputs.relations)
        changed_relations[3] = replace(changed_relations[3], relation_oid=changed_relations[3].relation_oid + 1)
        async with fixture.operator.transaction(isolation="repeatable_read"):
            with pytest.raises(ValueError):
                await prepare_em_provider_inputs(
                    fixture.operator,
                    EMProviderInputs(tuple(changed_relations)),
                    runtime_roles=(fixture.roles["reader"],),
                )
        await fixture.admin.execute(f"TRUNCATE {fixture.inputs.relation('npi')}")
        with pytest.raises(ValueError):
            await _em_provider_reader_receipt(fixture)
        (tmp_path / "provider-input-native.json").write_text(
            json.dumps(
                {
                    "receipt": receipt,
                    "roles": fixture.roles,
                    "ordinary_login_operator": True,
                    "actual_verified_reader_pool": True,
                    "native_direct_denials_sqlstate": "42501",
                    "exact_open_replay": True,
                    "failed_control_mutations_rolled_back": True,
                    "retained_tiger_parent_inheritance": True,
                    "stale_native_oid_and_filenode_refused": True,
                    "published_manifest_resolver_qualified": False,
                },
                indent=2,
            )
        )


async def test_native_published_em_provider_dependencies(custom_db, monkeypatch):
    from tests.managed_em_resolution_postgres_support import test_published_provider_resolver

    await test_published_provider_resolver(custom_db, monkeypatch)


async def test_native_managed_group_directory_source_policy_and_offices(source_admission, monkeypatch):
    from tests.managed_group_plan_postgres_support import (
        test_managed_group_handler_real_source_policy_and_retained_offices,
    )

    await test_managed_group_handler_real_source_policy_and_retained_offices(source_admission, monkeypatch)


@pytest.mark.usefixtures("billing_office_inputs")
async def test_native_managed_billing_retained_sql(source_admission, monkeypatch):
    from tests.managed_billing_sql_postgres_support import test_native_billing_retained_offices_sources_and_lineage

    await test_native_billing_retained_offices_sources_and_lineage(source_admission, monkeypatch)
