# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real migrated, published heap and claims SQL; clinical code mapping is synthetic."""

import json
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import pricing
from api.network_address_scope import network_address_read_scope
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_serving_read import resolve_network_serving_manifest
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_membership_candidate_indexes_postgres import _project_validated
from tests.test_network_membership_publication_postgres import _candidate_writer_roles, _publish
from tests.test_network_membership_serving_indexes_postgres import _full_source, _prepare
from tests.test_network_membership_validation_postgres import _batch
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_routes_postgres import _reader_engine
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_pricing_procedure_fast_path import _resolve_single_internal_code

pytestmark = pytest.mark.asyncio


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
    tables = await _seed_provider_claims(fixture, parent_engine, monkeypatch)
    reader_engine = None
    try:
        async with _candidate_writer_roles(fixture):
            async with fixture.connection.transaction():
                await _publish(fixture)
            for table in tables:
                await fixture.connection.execute(
                    f'GRANT SELECT ON "{fixture.control_schema}"."{table.name}" TO "{fixture.writer_roles["reader"]}"'
                )
            reader_engine = _reader_engine(parent_engine, fixture.writer_roles["reader"])
            async with fixture.observer.transaction(readonly=True):
                pin = await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema)
            try:
                yield SimpleNamespace(fixture=fixture, pin=pin, engine=reader_engine, second_selected=request.param)
            finally:
                await reader_engine.dispose()
                reader_engine = None
    finally:
        if reader_engine is not None:
            await reader_engine.dispose()


async def _claims(context, *, selected_network_ids=(42,), **filters):
    async with async_sessionmaker(context.engine)() as session, session.begin():
        if hasattr(context, "zip_schema"):
            await session.execute(text(f'SET LOCAL search_path TO "{context.zip_schema}", public'))
        assert await session.scalar(text("SELECT current_user")) == context.fixture.writer_roles["reader"]
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
            reply = await pricing.list_providers_by_procedure.__wrapped__(document)
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
