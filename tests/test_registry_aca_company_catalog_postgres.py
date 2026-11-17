# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native HIOS evidence and company-aware catalogue reads without manual approvals."""

import json
from uuid import UUID

import pytest

from api.provider_detail_network_read import _selected_networks
from process.network_fhir_source_witness_contract import digest
from process.registry_aca_company_catalog import enrich_aca_company_sources
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_effective_network_catalog import effective_catalog_sql
from process.registry_native_network_catalog import (
    native_catalog_descriptor,
    native_catalog_rows,
    native_network_definitions,
)
from process.registry_network_catalog_read import RegistryNetworkCatalogQuery, _page
from tests.effective_catalog_support import GENERATION, manual_record
from tests.effective_catalog_support import effective_catalog_db as effective_catalog_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_imported_source_inventory import native_aca_source
from tests.test_registry_issuer_resolution_postgres import _company_group, _evidence, _source

pytestmark = pytest.mark.asyncio


async def source_company(connection, schema):
    company_id, group_id = await _company_group(connection, schema)
    snapshot = await _source(connection, schema)
    key = await _evidence(connection, schema, snapshot, company_id, group_id, issuer=10001)
    await connection.execute(
        f'UPDATE "{schema}".registry_source_observation SET observation_json='
        "observation_json||jsonb_build_object('group_kind','naic_group') WHERE snapshot_id=$1 AND source_record_key=$2",
        snapshot,
        key,
    )
    source = native_aca_source()
    source["definitions"][0]["state"] = "CA"
    async with connection.transaction(isolation="repeatable_read"):
        (enriched,) = await enrich_aca_company_sources(connection, [source], control_schema=schema)
    return enriched, company_id, group_id


async def test_hios_source_period_ignores_pending_draft_corrections(serving_schema):
    connection, schema, _engine = serving_schema
    source, company_id, group_id = await source_company(connection, schema)
    metadata = source["company_catalog"]
    assert metadata["company_record"]["company_id"] == str(company_id)
    assert metadata["company_record"]["display_name"] == "Source legal company"
    assert metadata["group_record"]["display_name"] == "Source reported group"
    assert metadata["resolution"]["reporting_year"] == 2024
    assert metadata["group_record"]["group_kind"] == "naic_group"
    await connection.execute(
        f"UPDATE \"{schema}\".company_registry SET display_name='Pending company name' WHERE company_id=$1", company_id
    )
    await connection.execute(
        f"UPDATE \"{schema}\".company_group_registry SET display_name='Pending group name',group_kind='corporate_parent' WHERE group_id=$1",
        group_id,
    )
    async with connection.transaction(isolation="repeatable_read"):
        (again,) = await enrich_aca_company_sources(connection, [source], control_schema=schema)
    assert again["company_catalog"] == metadata
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0


async def seed_projection(fixture, source):
    descriptor = native_catalog_descriptor(source)
    generation = digest(descriptor)
    projection = native_catalog_rows(
        descriptor,
        generation=generation,
        metadata_generation=GENERATION,
        accepted_at="2026-11-17T00:00:00Z",
        networks={key: 7 for key in native_network_definitions(descriptor)},
    )
    catalog = '"' + fixture.catalog + '"'
    await fixture.connection.execute(f"DELETE FROM {catalog}.registry_fhir_catalog_binding")
    await fixture.connection.execute(f"DELETE FROM {catalog}.registry_fhir_catalog_source")
    await fixture.connection.execute(
        f"INSERT INTO {catalog}.registry_fhir_catalog_source VALUES($1,$2,$3::jsonb,'published_normalized',1,1,now(),now())",
        generation,
        source["source_id"],
        json.dumps(descriptor),
    )
    for row in projection.binding_rows:
        await fixture.connection.execute(
            f"INSERT INTO {catalog}.registry_fhir_catalog_binding VALUES($1,$2,$3,$4,$5::jsonb)", *row
        )
    for row in projection.relationship_rows:
        await fixture.connection.execute(
            f"INSERT INTO {catalog}.registry_fhir_catalog_relationship VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)",
            *row,
        )


async def test_aca_company_network_search_and_exclusions_work_without_approved_company(
    serving_schema, effective_catalog_db
):
    connection, schema, _engine = serving_schema
    native_source, company_id, group_id = await source_company(connection, schema)
    fixture = effective_catalog_db
    await seed_projection(fixture, native_source)
    page = await _page(
        fixture.connection,
        fixture.manifest,
        RegistryNetworkCatalogQuery(),
        (),
        fixture.control,
    )
    assert page["total"] == 1 and page["items"][0]["network_id"] == 7
    assert page["items"][0]["companies"][0]["company_id"] == str(company_id)
    assert page["items"][0]["companies"][0]["group_id"] == str(group_id)
    sql = await effective_catalog_sql(fixture.connection, fixture.manifest, fixture.control)
    catalog_records = await fixture.connection.fetch("SELECT record_kind,record_key,record_json FROM " + sql)
    companies = [entry for entry in catalog_records if entry["record_kind"] == "company"]
    assert len(companies) == 1 and companies[0]["record_key"] == str(company_id)
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control}".registry_approved_record') == 0
    policy = RegistryOrganizationVisibility(excluded_company_ids=(str(company_id),))
    assert await _selected_networks(fixture.connection, (7,), fixture.manifest, (), fixture.control, policy) == (7,)
    hidden = await _page(
        fixture.connection,
        fixture.manifest,
        RegistryNetworkCatalogQuery(),
        (),
        fixture.control,
        organization_policy=policy,
    )
    assert hidden["total"] == 1 and hidden["items"][0]["companies"] == []
    await manual_record(
        fixture,
        "company",
        company_id,
        {"display_name": "Approved company name", "roles": ["insurer"], "aliases": [], "archived": False},
    )
    renamed = await _page(
        fixture.connection,
        fixture.manifest,
        RegistryNetworkCatalogQuery(),
        (),
        fixture.control,
    )
    assert renamed["items"][0]["companies"][0]["display_name"] == "Approved company name"
