# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address eligibility is applied before office and report selection."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import column, select, table
from sqlalchemy.dialects.postgresql import dialect

from api import network_provider_read as readers
from api.endpoint import hospital_prices, network_providers, pharmacy_license, reports
from api.network_address_scope import NetworkAddressReadScope
from db.models import PharmacyLicenseRecord
from process import registry_effective_network_catalog as catalog
from tests.test_network_provider_membership_page_postgres import membership_page_db as membership_page_db
from tests.test_network_raw_provider_api_identity import _manifest, _office_spies, _provider_page
from tests.test_pharmacy_license_api import FakeResult, make_request


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,minimum", [("office", 0), ("office", 3), ("detail", 0), ("detail", 3), ("provider", 0)])
@pytest.mark.parametrize("failure", [None, "records", "networks", "page"])
async def test_catalog_backed_endpoint_keeps_scope_through_filtered_reads(monkeypatch, kind, minimum, failure):
    manifest = replace(_manifest(), source_generations={"fhir_catalog": "a" * 64})
    driver, session = object(), object()
    before = (catalog.current_catalog_records_sql(), catalog.current_catalog_networks_sql())
    catalog_records_sql, catalog_networks_sql = (
        "(SELECT * FROM synthetic_records)",
        "(SELECT * FROM synthetic_networks)",
    )

    async def projection(actual, selected, control, *, stage, result):
        assert actual is driver and selected is manifest and control
        if failure == stage:
            raise ValueError("catalog unavailable")
        return result

    @asynccontextmanager
    async def snapshot(actual):
        assert actual is session
        yield driver

    async def page(actual, scope, **options):
        assert actual is driver and scope.manifest is manifest and scope.network_ids == (7,)
        assert (catalog.current_catalog_records_sql(), catalog.current_catalog_networks_sql()) == (
            catalog_records_sql,
            catalog_networks_sql,
        )
        if kind != "provider":
            assert options["office_filters"].min_address_sources == minimum
        if failure == "page":
            raise network_providers.NetworkProviderReadUnavailable("page unavailable")
        return {"generation_id": 7, "total_count": 0, "providers": []}

    monkeypatch.setattr(network_providers, "require_registry_auth", lambda _request: None)
    monkeypatch.setattr(network_providers, "bind_network_request_reader", AsyncMock(return_value=session))
    monkeypatch.setattr(network_providers, "network_serving_read_context", snapshot)
    monkeypatch.setattr(network_providers, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    for name in ("read_network_provider_page", "read_network_provider_detail", "read_network_provider_only_page"):
        monkeypatch.setattr(network_providers, name, AsyncMock(side_effect=page))
    monkeypatch.setattr(
        catalog, "effective_catalog_sql", lambda *args: projection(*args, stage="records", result=catalog_records_sql)
    )
    monkeypatch.setattr(
        catalog,
        "effective_catalog_networks_sql",
        lambda *args: projection(*args, stage="networks", result=catalog_networks_sql),
    )
    query = f"network_ids=7&network_generation=7&min_address_sources={minimum}"
    if kind == "provider":
        query += "&membership_level=provider"
    request = SimpleNamespace(body=b"", headers={}, query_string=query)
    options = {"provider_system": "npi", "provider_id": "1000000004"} if kind == "detail" else {}
    reply = await network_providers._provider_response(request, **options)
    assert reply.status == (503 if failure else 200)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert (catalog.current_catalog_records_sql(), catalog.current_catalog_networks_sql()) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("compact", [False, True])
async def test_network_minimum_qualifies_offices_before_membership_pages(monkeypatch, compact):
    manifest = _manifest(compact=compact)
    object.__setattr__(manifest, "_supports_membership_pages", True)
    connection, _, _ = _office_spies(monkeypatch, manifest)
    await readers.read_network_provider_page(
        connection,
        NetworkAddressReadScope(manifest, (11,), None),
        limit=1,
        office_filters=readers.NetworkOfficeSelection(min_address_sources=3),
    )
    query, *arguments = connection.fetchrow.await_args.args
    assert query.startswith("WITH page_addresses")
    _, page_arguments = readers._provider_addresses(
        manifest, (11,), location_lookup=True, columns=readers._PAGE_ADDRESS_COLUMNS
    )
    _, qualification_arguments = readers._provider_addresses(
        manifest,
        None,
        first_parameter=13 + len(page_arguments),
        visible_membership_sql="TRUE",
        columns=readers._QUALIFYING_ADDRESS_COLUMNS,
    )
    assert tuple(arguments[12:]) == page_arguments + qualification_arguments
    assert query.index("address.source_count >= 3") < query.index("identities AS") < query.index("LIMIT $4")
    assert "qualifying_memberships AS" not in query
    assert "JOIN qualifying_addresses qualified_address" in query


@pytest.mark.asyncio
async def test_filtered_network_detail_retains_original_authorized_identity(monkeypatch):
    manifest = _manifest()
    connection, _, _ = _office_spies(monkeypatch, manifest)
    document = _provider_page()
    document["providers"][0]["locations"] = []
    monkeypatch.setattr(readers, "_result", lambda _receipt: document)
    result = await readers.read_network_provider_detail(
        connection,
        NetworkAddressReadScope(manifest, (11,), None),
        provider_system="npi",
        provider_id="1000000491",
        office_filters=readers.NetworkOfficeSelection(min_address_sources=3),
    )
    assert result["provider"]["locations"] == []
    query = connection.fetchrow.await_args.args[0]
    assert "FROM eligible GROUP BY provider_system,provider_id" in query
    assert "SELECT count(*) FROM offices office" in query


@pytest.mark.parametrize("value", [-1, True, 2.5, 2147483648])
def test_typed_network_minimum_rejects_invalid_values(value):
    with pytest.raises(readers.NetworkProviderReadError, match="source threshold"):
        readers.NetworkOfficeSelection(min_address_sources=value).arguments()


def test_network_provider_only_results_cannot_claim_an_address_filter():
    request = SimpleNamespace(body=b"", query_string="network_ids=42&min_address_sources=3", headers={})
    assert network_providers._parameters(request, is_detail=False)["office_filters"] == {"min_address_sources": 3}
    request.query_string += "&membership_level=provider"
    with pytest.raises(ValueError):
        network_providers._parameters(request, is_detail=False)


@pytest.mark.parametrize("source", ["mrf.entity_address_unified", "mrf.npi_address"])
def test_report_minimum_filters_input_relation_before_primary_office_and_aggregates(source):
    baseline, baseline_params = reports._build_chain_summary_sql(
        names=["sample"], has_staffing_helper=False, include_states=True, address_table_sql=source
    )
    zero, zero_params = reports._build_chain_summary_sql(
        names=["sample"],
        has_staffing_helper=False,
        include_states=True,
        address_table_sql=source,
        min_address_sources=0,
    )
    assert (zero, zero_params) == (baseline, baseline_params)
    filtered, params = reports._build_chain_summary_sql(
        names=["sample"],
        has_staffing_helper=False,
        include_states=True,
        address_table_sql=source,
        min_address_sources=3,
    )
    predicate = "source_address.source_count >= 3" if "unified" in source else "FALSE"
    assert f"(SELECT * FROM {source} source_address WHERE {predicate})" in filtered
    assert params["min_address_sources"] == 3


@pytest.mark.asyncio
async def test_native_license_evidence_matches_complete_office_not_only_npi(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_is_table_available", AsyncMock(return_value=True))
    clause = await pharmacy_license._license_address_source_filter(object(), PharmacyLicenseRecord.__table__, 3)
    compiled = clause.compile(dialect=dialect())
    sql = str(compiled)
    assert "EXISTS (SELECT 1" in sql and "source_count >=" in sql
    for field in ("address_line1", "address_line2", "city", "state", "zip_code"):
        assert "pharmacy_license_record_v1." + field in sql
    assert "source_count >= 3" in sql


@pytest.mark.asyncio
async def test_unknown_license_evidence_returns_empty_address_records_and_provider_identity(monkeypatch):
    monkeypatch.setattr(pharmacy_license, "_is_table_available", AsyncMock(side_effect=[True, False]))
    request = make_request([FakeResult()], {"min_address_sources": "3", "include_history": "false"})
    payload = json.loads((await pharmacy_license.get_pharmacy_license_by_npi(request, "1000000491")).body)
    assert payload["npi"] == 1000000491 and payload["licenses"] == [] and payload["summary"]["total_licenses"] == 0


@pytest.mark.asyncio
async def test_hospital_source_address_without_stored_evidence_does_not_qualify(monkeypatch):
    reader = AsyncMock(side_effect=AssertionError("source address has no stored evidence"))
    monkeypatch.setattr(hospital_prices, "list_hospital_price_status_page", reader)
    result = await hospital_prices.search_hospital_price_facilities(
        SimpleNamespace(args={"min_address_sources": "3", "include_metadata": "true"})
    )
    assert result.status == 200 and json.loads(result.body) == {"items": [], "next_cursor": None}
    reader.assert_not_awaited()
    invalid = await hospital_prices.search_hospital_price_facilities(
        SimpleNamespace(args={"min_address_sources": "-1"})
    )
    assert invalid.status == 400


async def _native_filtered_page(case, minimum, *, selector=(None, None), limit=1, offset=0):
    addresses, address_arguments = readers._provider_addresses(case.manifest, (42, 88))
    query = readers._source_filtered_provider_sql(readers._PROVIDERS_SQL, minimum, selector != (None, None)).format(
        namespace=case.namespace, addresses=addresses, address_membership="TRUE"
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        receipt = await case.connection.fetchrow(
            query,
            "{42,88}",
            *selector,
            limit,
            offset,
            readers.MAX_LOCATIONS,
            readers.MAX_RESPONSE_BYTES,
            case.manifest.generation_id,
            None,
            None,
            None,
            None,
            *address_arguments,
        )
        return readers._result(receipt)


@pytest.mark.asyncio
async def test_native_network_minimum_counts_and_pages_alternate_qualifying_offices(membership_page_db):
    case = membership_page_db
    await case.connection.execute(f"UPDATE {case.namespace}.base_address SET source_count=2")
    await case.connection.execute(f"UPDATE {case.namespace}.custom_address SET source_count=2")
    await case.connection.execute(
        f"UPDATE {case.namespace}.base_address SET source_count=CASE WHEN location_key=$1 THEN 3 ELSE 4 END "
        "WHERE location_key=ANY($2::text[])",
        f"{101:064x}",
        [f"{101:064x}", f"{102:064x}"],
    )
    first = await _native_filtered_page(case, 3)
    second = await _native_filtered_page(case, 3, offset=1)
    assert first["total_count"] == second["total_count"] == 2
    assert first["has_more"] is True and second["has_more"] is False
    assert first["providers"][0]["provider_id"] == "1000000491"
    alternate = second["providers"][0]
    assert alternate["provider_id"] == "provider-a"
    assert [office["location_key"] for office in alternate["locations"]] == [f"{101:064x}"]
    assert (await _native_filtered_page(case, 4))["total_count"] == 1
    assert (await _native_filtered_page(case, 5))["total_count"] == 0


@pytest.mark.asyncio
async def test_native_network_detail_retains_identity_after_all_offices_fail_minimum(membership_page_db):
    case = membership_page_db
    await case.connection.execute(f"UPDATE {case.namespace}.base_address SET source_count=2")
    page = await _native_filtered_page(case, 3, selector=("provider_directory", "provider-a"))
    assert page["total_count"] == 1
    assert page["providers"][0]["provider_id"] == "provider-a"
    assert page["providers"][0]["locations"] == []
    missing = await _native_filtered_page(case, 3, selector=("provider_directory", "missing"))
    assert missing["providers"] == [] and missing["total_count"] == 0


@pytest.mark.asyncio
async def test_native_license_qualifies_only_the_same_complete_office(membership_page_db, monkeypatch):
    fixture = membership_page_db
    await fixture.connection.execute(
        f"CREATE TABLE {fixture.namespace}.native_license(record_id integer,npi bigint,address_line1 text,"
        "address_line2 text,city text,state text,zip_code text)"
    )
    await fixture.connection.execute(
        f"INSERT INTO {fixture.namespace}.native_license VALUES "
        "(1,1000000491,'1 Example Street',NULL,'Sample City','CA','90210-1234'),"
        "(2,1000000491,'99 Other Street',NULL,'Sample City','CA','90210')"
    )
    await fixture.connection.execute(
        f"UPDATE {fixture.namespace}.base_address SET source_count=2,state_name='California',state_code='CA',zip5='90210' "
        "WHERE npi=1000000491"
    )
    address_table = table(
        "base_address",
        *(
            column(name)
            for name in (
                "npi",
                "inferred_npi",
                "first_line",
                "second_line",
                "city_name",
                "state_name",
                "state_code",
                "zip5",
                "source_count",
            )
        ),
        schema=fixture.manifest.schema_name,
    )
    native_license = table(
        "native_license",
        *(column(name) for name in ("record_id", "npi", "address_line1", "address_line2", "city", "state", "zip_code")),
        schema=fixture.manifest.schema_name,
    )
    monkeypatch.setattr(
        pharmacy_license, "EntityAddressUnified", SimpleNamespace(__tablename__="base_address", __table__=address_table)
    )
    monkeypatch.setattr(pharmacy_license, "_is_table_available", AsyncMock(return_value=True))
    predicate = await pharmacy_license._license_address_source_filter(object(), native_license, 3)
    compiled = str(
        select(native_license.c.record_id)
        .where(predicate)
        .compile(dialect=dialect(), compile_kwargs={"literal_binds": True})
    )
    assert await fixture.connection.fetch(compiled) == []
    await fixture.connection.execute(f"UPDATE {fixture.namespace}.base_address SET source_count=3 WHERE npi=1000000491")
    assert [license_record["record_id"] for license_record in await fixture.connection.fetch(compiled)] == [1]
