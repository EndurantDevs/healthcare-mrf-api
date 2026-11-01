# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real migrated, published heap and claims SQL; clinical code mapping is synthetic."""

import asyncio
import importlib
import json
import os
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import pricing
from api.network_address_scope import network_address_read_scope
from db.connection import Database
from process import native_publication_read_access
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_serving_read import resolve_network_serving_manifest
from tests.claims_pricing_contract_fakes import AsyncTransaction
from tests.signed_request_reader_postgres_support import native_reader_witness
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_membership_candidate_indexes_postgres import _project_validated
from tests.test_network_membership_publication_postgres import _candidate_writer_roles, _publish
from tests.test_network_membership_serving_indexes_postgres import _full_source, _prepare
from tests.test_network_membership_validation_postgres import _batch
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_pricing_procedure_fast_path import _resolve_single_internal_code

pytestmark = pytest.mark.asyncio
claims_pricing = importlib.import_module("process.claims_pricing")

CLAIMS_READ_COLUMNS = {
    "pricing_provider": ("city", "npi", "provider_name", "provider_type", "state", "year", "zip5"),
    "pricing_provider_procedure": (
        "npi",
        "procedure_code",
        "total_allowed_amount",
        "total_beneficiaries",
        "total_services",
        "total_submitted_charges",
        "year",
    ),
}


async def _seed_claim_offices(fixture, second_selected):
    """Validate, index and prepare the genuine source before any publication."""
    await _full_source(fixture)
    source_table = f'"{fixture.control_schema}".retained_addresses'
    await fixture.connection.execute(
        f"""UPDATE {source_table} SET first_line=CASE WHEN location_key=$1 THEN '10 Example Street' ELSE '20 Example Street' END,
        city_name=CASE WHEN location_key=$1 THEN 'Selected City' ELSE 'Other City' END,
        state_name=CASE WHEN location_key=$1 THEN 'NY' ELSE 'NJ' END,
        postal_code=CASE WHEN location_key=$1 THEN '10001' ELSE '07001' END,
        zip5=CASE WHEN location_key=$1 THEN '10001' ELSE '07001' END,
        lat=CASE WHEN location_key=$1 THEN 40.75 ELSE 41.75 END,
        long=CASE WHEN location_key=$1 THEN -73.99 ELSE -74.99 END""",
        "a" * 64,
    )
    if second_selected:
        await fixture.connection.execute(
            f'INSERT INTO "{fixture.copy_target.schema_name}".network_membership '
            "(network_id,provider_system,provider_id,location_id,evidence_id) VALUES(42,'npi','provider-a',$1,'second-office')",
            fixture.location_ids[1],
        )
        await _batch(fixture, 1)
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=5,accepted_rows=5 '
            "WHERE candidate_id=$1",
            UUID(fixture.copy_target.candidate_id),
        )
    await _project_validated(fixture)
    async with fixture.connection.transaction():
        await prepare_network_candidate_indexes(
            fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await _prepare(fixture)


async def _seed_provider_claims(fixture, parent_engine, monkeypatch):
    """Create native provider-grain claims with an excluded high-volume provider."""
    tables = [
        model.to_metadata(MetaData(), schema=fixture.control_schema)
        for model in (pricing.provider_table, pricing.provider_procedure_table)
    ]
    async with parent_engine.begin() as connection:
        for table in tables:
            await connection.run_sync(table.create)
        await connection.execute(
            tables[0].insert(),
            [
                {
                    "provider_key": 1,
                    "npi": 1111111111,
                    "year": 2023,
                    "provider_name": "Selected Provider",
                    "provider_type": "Synthetic",
                    "city": "Other City",
                    "state": "NJ",
                    "zip5": "07001",
                },
                {
                    "provider_key": 2,
                    "npi": 2222222222,
                    "year": 2023,
                    "provider_name": "Excluded Provider",
                    "provider_type": "Synthetic",
                    "city": "Selected City",
                    "state": "NY",
                    "zip5": "10001",
                },
            ],
        )
        await connection.execute(
            tables[1].insert(),
            [
                {
                    "npi": npi,
                    "year": 2023,
                    "procedure_code": 1099885900,
                    "total_services": services,
                    "total_submitted_charges": services * 120,
                    "total_allowed_amount": services * 90,
                    "total_beneficiaries": services / 2,
                }
                for npi, services in ((1111111111, 10), (2222222222, 10000))
            ],
        )
    monkeypatch.setattr(pricing, "provider_table", tables[0])
    monkeypatch.setattr(pricing, "provider_procedure_table", tables[1])
    monkeypatch.setattr(pricing, "_resolve_internal_codes_for_request", _resolve_single_internal_code)

    async def unavailable_provider_peer(*_arguments, **_options):
        """No fabricated peer-cost or office-specific price evidence in this fixture."""
        return None

    monkeypatch.setattr(pricing, "_enrich_provider_service_cost_indices", unavailable_provider_peer)
    return tables


@pytest.fixture(params=(False, True), ids=("excluded_second_office", "two_selected_offices"))
async def claims_offices(validation_db, serving_schema, monkeypatch, request):
    """Use actual sealed native publication and its restricted Reader privileges."""
    fixture = validation_db
    await _seed_claim_offices(fixture, request.param)
    _, _, parent_engine = serving_schema
    async with _candidate_writer_roles(fixture):
        database = Database()
        try:
            async with fixture.connection.transaction():
                await _publish(fixture)
            # Claims must be absent when the network fixture grants its broad metadata reads.
            tables = await _seed_provider_claims(fixture, parent_engine, monkeypatch)
            reader = fixture.writer_roles["reader"]
            password = secrets.token_hex(32)
            await fixture.connection.execute(f"ALTER ROLE \"{reader}\" LOGIN PASSWORD '{password}'")
            for table in tables:
                columns = ",".join(CLAIMS_READ_COLUMNS[table.name])
                await fixture.connection.execute(
                    f'GRANT SELECT ({columns}) ON "{fixture.control_schema}"."{table.name}" TO "{reader}"'
                )
            for key, environment_value in {
                "HOST": parent_engine.url.host,
                "PORT": parent_engine.url.port,
                "USER": parent_engine.url.username,
                "DATABASE": parent_engine.url.database,
                "READER_USER": reader,
                "READER_PASSWORD": password,
            }.items():
                if environment_value is not None:
                    monkeypatch.setenv("HLTHPRT_DB_" + key, str(environment_value))
            async with fixture.observer.transaction(readonly=True):
                pin = await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema)
            yield SimpleNamespace(fixture=fixture, pin=pin, database=database, second_selected=request.param)
        finally:
            await database.disconnect()


async def test_claims_fixture_preserves_default_port_when_source_dsn_omits_it(monkeypatch):
    module = importlib.import_module(__name__)
    fixture = SimpleNamespace(
        connection=SimpleNamespace(transaction=AsyncTransaction, execute=AsyncMock()),
        observer=SimpleNamespace(transaction=lambda **_options: AsyncTransaction()),
        writer_roles={"reader": "synthetic_reader"},
        control_schema="synthetic_control",
    )
    for name in ("_seed_claim_offices", "_publish", "resolve_network_serving_manifest"):
        monkeypatch.setattr(module, name, AsyncMock())
    monkeypatch.setattr(module, "_seed_provider_claims", AsyncMock(return_value=[]))
    monkeypatch.setattr(module, "_candidate_writer_roles", lambda _fixture: AsyncTransaction())
    monkeypatch.setattr(secrets, "token_hex", lambda _size: "synthetic-reader-password")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5432")
    parent = SimpleNamespace(url=make_url("postgresql://writer@database.example.test/synthetic"))
    stream = claims_offices.__wrapped__(fixture, (None, None, parent), monkeypatch, SimpleNamespace(param=False))
    try:
        context = await anext(stream)
        assert context.fixture is fixture
        assert os.environ["HLTHPRT_DB_PORT"] == "5432"
        assert os.environ["HLTHPRT_DB_HOST"] == "database.example.test"
        assert os.environ["HLTHPRT_DB_USER"] == "writer"
        assert os.environ["HLTHPRT_DB_DATABASE"] == "synthetic"
        assert os.environ["HLTHPRT_DB_READER_PASSWORD"] == "synthetic-reader-password"
    finally:
        await stream.aclose()


async def _claims(context, *, selected_network_ids=(42,), import_context=None, **filters):
    async with context.database.reader_session() as session:
        if hasattr(context, "zip_schema"):
            await session.execute(text(f'SET LOCAL search_path TO "{context.zip_schema}", public'))
        await native_reader_witness(session, context.fixture.writer_roles["reader"])
        document = SimpleNamespace(
            args={
                "code": "99213",
                "code_system": "CPT",
                "year": "2023",
                "limit": "1",
                "offset": "0",
                "order_by": "total_services",
                "network_ids": ",".join(map(str, selected_network_ids)),
                "include_evidence": "true",
                **filters,
            },
            ctx=SimpleNamespace(sa_session=session),
        )
        with network_address_read_scope(context.pin, selected_network_ids):
            # The genuine pin was resolved above; only HTTP/control-token middleware is outside this component.
            if import_context is None:
                reply = await pricing.list_providers_by_procedure.__wrapped__(document)
            else:
                reply = await pricing.list_providers_by_procedure.__wrapped__(
                    document, native_args=document.args, import_context=import_context
                )
        return json.loads(reply.body)


async def test_native_equal_claim_totals_have_stable_adjacent_pages(claims_offices):
    context = claims_offices
    await context.fixture.connection.execute(
        f'UPDATE "{context.fixture.control_schema}".pricing_provider_procedure '
        "SET total_services=10, total_submitted_charges=1200, total_allowed_amount=900, total_beneficiaries=5 "
        "WHERE npi=2222222222"
    )
    for order_by in ("total_services", "total_allowed_amount"):
        for order in ("asc", "desc"):
            pages = [
                await _claims(
                    context, selected_network_ids=(42, 88), order_by=order_by, order=order, offset=str(offset)
                )
                for offset in (0, 1)
            ]
            assert [page["pagination"]["total"] for page in pages] == [2, 2]
            assert [page["items"][0]["npi"] for page in pages] == [1111111111, 2222222222]
            assert [page["items"][0]["total_services"] for page in pages] == [10, 10]
            assert [page["items"][0]["total_allowed_amount"] for page in pages] == [900, 900]
            assert [office["location_key"] for office in pages[1]["items"][0]["locations"]] == ["c" * 64]


async def test_native_same_office_filter_count_page_and_original_claim_grain(claims_offices):
    context = claims_offices
    page = await _claims(context)
    assert page["pagination"]["total"] == 1 and len(page["items"]) == 1
    item = page["items"][0]
    assert item["npi"] == 1111111111 and item["total_services"] == 10
    expected = ["a" * 64, "b" * 64] if context.second_selected else ["a" * 64]
    assert [office["location_key"] for office in item["locations"]] == expected
    assert item["claims_geography"] == {"city": "Other City", "state": "NJ", "zip5": "07001"}
    assert not {"city", "state", "zip5", "priceable", "in_network"}.intersection(item)
    assert page["evidence"]["matched_provider_count"] == 1
    # One selected office must satisfy the conjunction; different offices cannot contribute different predicates.
    split = await _claims(context, city="Selected City", zip5="07001", zip_radius_miles="0")
    assert split["pagination"]["total"] == 0 and split["items"] == []
    selected = await _claims(context, state="NY", city="Selected City", zip5="10001", zip_radius_miles="0")
    assert selected["pagination"]["total"] == 1
    assert [office["location_key"] for office in selected["items"][0]["locations"]] == ["a" * 64]
    other = await _claims(context, state="NJ", city="Other City", zip5="07001", zip_radius_miles="0")
    assert other["pagination"]["total"] == int(context.second_selected)
    geographic = await _claims(context, lat="40.75", long="-73.99", radius_miles="1")
    assert geographic["pagination"]["total"] == 1
    assert [office["location_key"] for office in geographic["items"][0]["locations"]] == ["a" * 64]
    beyond = await _claims(context, offset="1")
    assert beyond["pagination"]["total"] == 1 and beyond["items"] == []


@pytest.fixture(params=("divergent", "absent"), ids=("divergent_geo_zip", "absent_geo_zip"))
async def zip_claims_offices(claims_offices, serving_schema, monkeypatch, request):
    """Retain real ZCTA input in the fixture-owned schema, not a mocked lookup."""
    context = claims_offices
    fixture = context.fixture
    schema = fixture.control_schema
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".zcta5 (zcta5ce text PRIMARY KEY, intptlat text, intptlon text)'
    )
    await fixture.connection.execute(f'INSERT INTO "{schema}".zcta5 VALUES ($1,$2,$3)', "10001", "40.75", "-73.99")
    await fixture.connection.execute(f'GRANT SELECT ON "{schema}".zcta5 TO "{fixture.writer_roles["reader"]}"')
    alternate_table = pricing.geo_zip_table.to_metadata(MetaData(), schema=schema)
    if request.param == "divergent":
        async with serving_schema[2].begin() as connection:
            await connection.run_sync(alternate_table.create)
            await connection.execute(
                alternate_table.insert(),
                {"zip_code": "10001", "city": "Other City", "state": "NJ", "latitude": 41.75, "longitude": -74.99},
            )
        await fixture.connection.execute(
            f'GRANT SELECT ON "{schema}".geo_zip_lookup TO "{fixture.writer_roles["reader"]}"'
        )
    else:
        assert await fixture.connection.fetchval("SELECT to_regclass($1)", f"{schema}.geo_zip_lookup") is None
    monkeypatch.setattr(pricing, "geo_zip_table", alternate_table)
    context.zip_schema = schema
    return context


async def test_native_canonical_zip_matches_nearby_zcta_without_alternate_centroid(zip_claims_offices):
    context = zip_claims_offices
    page = await _claims(context, zip5="10001", zip_radius_miles="10")
    coordinate_page = await _claims(context, lat="40.75", long="-73.99", radius_miles="10")
    assert page["pagination"] == coordinate_page["pagination"] == {"total": 1, "limit": 1, "offset": 0, "page": 1}
    assert page["items"] == coordinate_page["items"]
    item = page["items"][0]
    assert item["npi"] == 1111111111 and item["total_services"] == 10
    assert [office["location_key"] for office in item["locations"]] == ["a" * 64]
    assert item["claims_geography"] == {"city": "Other City", "state": "NJ", "zip5": "07001"}
    assert page["evidence"]["matched_provider_count"] == 1
    beyond = await _claims(context, zip5="10001", zip_radius_miles="10", offset="1")
    assert beyond["pagination"]["total"] == 1 and beyond["items"] == []


async def test_native_import_relation_intersects_exact_network_offices_before_counts(claims_offices):
    from sqlalchemy import literal, select

    from api.custom_import_provider_service_sql import ProviderServiceImportQuery
    from process.custom_import.read_core import PreparedNpiEntityRelation

    imported_rows = (
        select(literal("1111111111").label("entity_value"))
        .union_all(select(literal("2222222222").label("entity_value")))
        .subquery()
    )
    imported = ProviderServiceImportQuery(
        PreparedNpiEntityRelation(select(imported_rows.c.entity_value), (), "a" * 64, "b" * 64), True
    )
    page = await _claims(claims_offices, import_context=imported, state="NY", zip5="10001", zip_radius_miles="0")
    assert page["pagination"]["total"] == 1
    assert [item["npi"] for item in page["items"]] == [1111111111]
    assert [office["location_key"] for office in page["items"][0]["locations"]] == ["a" * 64]
    hidden_only = ProviderServiceImportQuery(
        PreparedNpiEntityRelation(select(literal("2222222222").label("entity_value")), (), "c" * 64, "d" * 64), True
    )
    hidden = await _claims(claims_offices, import_context=hidden_only)
    assert hidden["pagination"]["total"] == 0 and hidden["items"] == []
    beyond = await _claims(claims_offices, import_context=imported, offset="1")
    assert beyond["pagination"]["total"] == 1 and beyond["items"] == []


@pytest.fixture
async def publication_claims(claims_offices, serving_schema, monkeypatch):
    context = claims_offices
    fixture = context.fixture
    schema = fixture.control_schema
    engine = serving_schema[2]
    context.classes = claims_pricing._staging_classes(uuid4().hex[:8], schema)
    context.names = tuple(getattr(claims_pricing, name).__main_table__ for name in context.classes)
    async with engine.begin() as connection:
        for class_name, staged in context.classes.items():
            live = getattr(claims_pricing, class_name).__table__.to_metadata(MetaData(), schema=schema)
            await connection.run_sync(live.create, checkfirst=True)
            await connection.run_sync(staged.__table__.create)
            await connection.execute(
                staged.__table__.insert().from_select([column.name for column in live.columns], select(live))
            )
            target = f'"{schema}"."{live.name}"'
            auditor = fixture.writer_roles["loader"]
            await connection.execute(text(f'GRANT SELECT ON {target} TO "{auditor}" WITH GRANT OPTION'))
            await connection.execute(text(f'GRANT MAINTAIN,UPDATE ON {target} TO "{auditor}"'))
    publisher = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    monkeypatch.setattr(claims_pricing, "db", publisher)
    return context


async def _state(context):
    fixture = context.fixture
    return await fixture.connection.fetchval(
        """SELECT jsonb_agg(jsonb_build_array(c.oid,c.relname,c.relowner,c.relacl::text,
          (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.attacl::text) ORDER BY a.attnum)
           FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)) ORDER BY c.relname)
          FROM pg_class c WHERE c.relnamespace=$1::regnamespace AND c.relkind='r'""",
        fixture.control_schema,
    )


async def _reads(context):
    connection = context.fixture.connection
    grants_by_table = {}
    for name in context.names:
        oid = await connection.fetchval("SELECT to_regclass($1)::oid", f"{context.fixture.control_schema}.{name}")
        rows = await connection.fetch(native_publication_read_access._READS_SQL, oid)
        grants_by_table[name] = sorted((tuple(row) for row in rows), key=repr)
    return grants_by_table


async def _publish_claims(context):
    await claims_pricing._publish_by_table_rename(context.classes, context.fixture.control_schema)


async def _assert_reader_denials(context):
    reader = await context.database._connect_reader()
    schema = context.fixture.control_schema
    async with reader.engine.connect() as connection:
        connection = await connection.execution_options(postgresql_readonly=False)
        for statement in (
            f'SELECT provider_key FROM "{schema}".pricing_provider',
            f'SELECT reported_code FROM "{schema}".pricing_provider_procedure',
            f'INSERT INTO "{schema}".pricing_provider(npi,year) VALUES(3333333333,2023)',
            f"UPDATE \"{schema}\".pricing_provider SET city='Forbidden'",
            f'DELETE FROM "{schema}".pricing_provider_procedure',
            f'TRUNCATE "{schema}".pricing_provider_procedure',
            f'CREATE TABLE "{schema}".forbidden(id int)',
        ):
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin():
                    await connection.execute(text(statement))
            assert denied.value.orig.sqlstate == "42501"


async def test_refresh_preserves_exact_reads_and_native_page_hydration(publication_claims):
    context = publication_claims
    fixture = context.fixture
    before = await _reads(context)
    original_page = await _claims(context)
    original_oids = await fixture.connection.fetch(
        "SELECT oid FROM pg_class WHERE relnamespace=$1::regnamespace AND relname=ANY($2::text[]) ORDER BY relname",
        fixture.control_schema,
        list(context.names),
    )
    await _publish_claims(context)
    assert await _reads(context) == before
    current_oids = await fixture.connection.fetch(
        "SELECT oid FROM pg_class WHERE relnamespace=$1::regnamespace AND relname=ANY($2::text[]) ORDER BY relname",
        fixture.control_schema,
        list(context.names),
    )
    assert len(current_oids) == 8 and all(old != new for old, new in zip(original_oids, current_oids, strict=True))
    assert await _claims(context) == original_page
    page = await _claims(context, selected_network_ids=(42, 88), offset="1")
    assert page["pagination"]["total"] == 2 and page["items"][0]["npi"] == 1111111111
    assert page["items"][0]["locations"] and page["items"][0]["provider_name"] == "Selected Provider"
    for name in context.names:
        relation = f"{fixture.control_schema}.{name}"
        assert not await fixture.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'UPDATE')", fixture.writer_roles["loader"], relation
        )
        assert not await fixture.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", fixture.writer_roles["reader"], relation
        )
    await _assert_reader_denials(context)


async def test_reader_contention_defers_publication_without_losing_stages(publication_claims, monkeypatch):
    context = publication_claims
    fixture = context.fixture
    for name in (
        "_ensure_live_code_tables",
        "_materialize_code_and_crosswalk_rows",
        "_materialize_procedure_provider_counts",
        "_materialize_procedure_taxonomy_signals",
        "_materialize_cost_level_rows",
        "_collect_cost_level_diagnostics",
        "_build_staging_indexes",
    ):
        monkeypatch.setattr(claims_pricing, name, AsyncMock())
    before = await _state(context)
    reads = await _reads(context)
    page = await _claims(context)
    async with context.database.reader_session() as reader:
        await native_reader_witness(reader, fixture.writer_roles["reader"])
        await reader.execute(text(f'SELECT npi FROM "{fixture.control_schema}".pricing_provider_procedure'))
        with pytest.raises(claims_pricing.Retry) as deferred:
            await claims_pricing._materialize_and_publish_claims(context.classes, fixture.control_schema)
        assert claims_pricing.postgres_sqlstate(deferred.value.__cause__) == "55P03"
        assert deferred.value.defer_score == claims_pricing.CLAIMS_FINISH_RETRY_SECONDS * 1000
        assert await _state(context) == before
        assert await _reads(context) == reads
    await claims_pricing._materialize_and_publish_claims(context.classes, fixture.control_schema)
    assert await _state(context) != before
    assert await _reads(context) == reads
    assert await _claims(context) == page
    await _assert_reader_denials(context)


@pytest.mark.parametrize("revocation", ["one_column", "all_claims_reads"])
async def test_refresh_preserves_revocation_and_cannot_provision_missing_reads(publication_claims, revocation):
    context = publication_claims
    fixture = context.fixture
    for name, columns in CLAIMS_READ_COLUMNS.items():
        if revocation == "one_column" and name != "pricing_provider":
            continue
        selected = "provider_name" if revocation == "one_column" else ",".join(columns)
        await fixture.connection.execute(
            f'REVOKE SELECT({selected}) ON "{fixture.control_schema}"."{name}" FROM "{fixture.writer_roles["reader"]}"'
        )
    before = await _reads(context)
    await _publish_claims(context)
    assert await _reads(context) == before
    with pytest.raises(DBAPIError) as denied:
        await _claims(context)
    assert denied.value.orig.sqlstate == "42501"
    await _assert_reader_denials(context)


@pytest.mark.parametrize("failure", ["foreign_grantor", "foreign_stage_owner", "cancel", "rename_dependency"])
async def test_late_failure_rolls_back_every_heap_and_acl(publication_claims, monkeypatch, failure):
    context = publication_claims
    fixture = context.fixture
    last_name = context.names[-1]
    stage = list(context.classes.values())[-1].__tablename__
    live_relation = f'"{fixture.control_schema}"."{last_name}"'
    expected = RuntimeError
    if failure == "foreign_grantor":
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.writer_roles["loader"]}"')
            await fixture.connection.execute(f'GRANT SELECT ON {live_relation} TO "{fixture.writer_roles["reader"]}"')
    elif failure == "foreign_stage_owner":
        await fixture.connection.execute(
            f'ALTER TABLE "{fixture.control_schema}"."{stage}" OWNER TO "{fixture.writer_roles["loader"]}"'
        )
    elif failure == "rename_dependency":
        await fixture.connection.execute(
            f'CREATE VIEW "{fixture.control_schema}".held_claims AS SELECT * FROM {live_relation}'
        )
        expected = DBAPIError
    else:
        preserve = claims_pricing.preserve_publication_reads

        async def cancel_after_grants(connection, **names):
            await preserve(connection, **names)
            if names["live_table"] == last_name:
                raise asyncio.CancelledError

        monkeypatch.setattr(claims_pricing, "preserve_publication_reads", cancel_after_grants)
        expected = asyncio.CancelledError
    try:
        before = await _state(context)
        with pytest.raises(expected):
            await _publish_claims(context)
        assert await _state(context) == before
        page = await _claims(context)
        assert page["pagination"]["total"] == 1 and page["items"][0]["npi"] == 1111111111
    finally:
        if failure == "foreign_grantor":
            async with fixture.connection.transaction():
                await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.writer_roles["loader"]}"')
                await fixture.connection.execute(
                    f'REVOKE SELECT ON {live_relation} FROM "{fixture.writer_roles["reader"]}"'
                )
