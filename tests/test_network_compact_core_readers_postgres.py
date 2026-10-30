# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real compact provider reads and exact retained-site adoption/carry."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.custom_import_provider_network import ProviderNetworkBinding, provider_network_read_scope
from api.network_address_scope import (
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_statement,
)
from api.network_compact_address import compact_address_native_sql
from api.network_provider_read import (
    NetworkOfficeSelection,
    NetworkProviderNotFound,
    NetworkProviderReadUnavailable,
    read_network_provider_detail,
    read_network_provider_page,
)
from db.models import EntityAddressUnified
from process.custom_import.read_contracts import CustomImportReadUnavailableError
from process.network_approved_site_source import copy_approved_site_rows
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions
from process.registry_source_site_catalog import RegistrySourceSiteCatalogUnavailable, read_registry_source_sites
from tests import test_network_compact_publication_postgres as publication
from tests.test_network_compact_custom_publication_postgres import _approved_candidate


@pytest.fixture
async def compact_core_db(monkeypatch):
    original_source = publication._full_two_offices

    async def prepare_source(connection, roles, original):
        schema = await original_source(connection, roles, original)
        await connection.execute(f"""UPDATE "{schema}".entity_address_unified
            SET location_key=encode(sha256(convert_to(location_key,'UTF8')),'hex'),
                entity_id='1234567893',npi=1234567893,entity_name='Example Provider'""")
        return schema

    monkeypatch.setattr(publication, "_full_two_offices", prepare_source)
    async with asynccontextmanager(publication.compact_publication_db.__wrapped__)(monkeypatch) as fixture:
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
        predecessor = await publication._candidate(fixture, (42, 88), 0)
        await publication._prepare(fixture, predecessor)
        await publication._seal(fixture, predecessor)
        await publication._publish(fixture, predecessor)
        async with publication._reader(fixture) as (_connection, driver):
            previous = await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema)
        fixture.target, _members = await _approved_candidate(fixture, 0, previous.generation_id, previous)
        await publication._prepare(fixture, fixture.target)
        await publication._seal(fixture, fixture.target)
        await publication._publish(fixture, fixture.target)
        yield fixture


@pytest.mark.asyncio
async def test_native_provider_catalog_adoption_and_bound_transport(compact_core_db):
    fixture = compact_core_db
    async with publication._reader(fixture) as (_connection, driver):
        manifest = await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema)
        with network_address_read_scope(manifest, (42,)) as scope:
            page = await read_network_provider_page(driver, scope, control_schema=fixture.control_schema)
            assert page["total_count"] == 1 and len(page["providers"][0]["locations"]) == 1
            provider = page["providers"][0]
            assert provider["provider_id"] == "1234567893"
            detail = await read_network_provider_detail(
                driver, scope, provider_system="npi", provider_id="1234567893", control_schema=fixture.control_schema
            )
            assert detail["provider"] == provider
            with pytest.raises(NetworkProviderNotFound):
                await read_network_provider_detail(
                    driver,
                    scope,
                    provider_system="npi",
                    provider_id="1000000004",
                    control_schema=fixture.control_schema,
                )
        catalog = await read_registry_source_sites(
            driver,
            generation_id=manifest.generation_id,
            provider_system="npi",
            provider_id="1234567893",
            control_schema=fixture.control_schema,
        )
        assert len(catalog["sites"]) == 2 and not catalog["has_more"]
        receipt = await _adopt_selected(fixture, driver, manifest, catalog, provider)
        other = next(
            site for site in catalog["sites"] if site["fields"]["location_id"] != receipt.records[0].location_id
        )
        with network_address_read_scope(manifest, (42,)) as scope:
            filtered = await read_network_provider_page(
                driver,
                scope,
                control_schema=fixture.control_schema,
                office_filters=NetworkOfficeSelection(other["fields"]["location_id"], 40.75, -73.5, 1),
            )
            assert filtered["total_count"] == 0 and filtered["providers"] == []
    await _transport_scope(fixture, manifest)
    await _copy_selected(fixture, receipt)
    await _refuse_retired(fixture, manifest, receipt)


async def _adopt_selected(fixture, driver, manifest, catalog, provider):
    selected_id = provider["locations"][0]["location_id"]
    selected = next(site["fields"] for site in catalog["sites"] if site["fields"]["location_id"] == selected_id)
    selection_by_field = {name: value for name, value in selected.items() if name != "source_generation"}
    receipt = await resolve_retained_site_adoptions(
        driver, manifest, json.dumps([selection_by_field]).encode(), control_schema=fixture.control_schema
    )
    assert receipt.address_table_oid == manifest.compact_layout.base.table_oid
    assert receipt.as_dict()["compact_address_relations"] == [
        ["base", fixture.address.schema, "entity_address_unified", manifest.address_table_oid]
    ]
    assert receipt.records[0].location_id == selected_id
    with pytest.raises(RetainedSiteAdoptionError):
        await resolve_retained_site_adoptions(
            driver,
            manifest,
            json.dumps([{**selection_by_field, "address_row_sha256": "f" * 64}]).encode(),
            control_schema=fixture.control_schema,
        )
    return receipt


async def _transport_scope(fixture, manifest):
    binding = ProviderNetworkBinding(manifest.generation_id, manifest.manifest_sha256, (42,), "a" * 64)
    async with async_sessionmaker(fixture.reader_engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        async with provider_network_read_scope(session, binding):
            statement = scoped_address_statement(select(EntityAddressUnified.__table__.c.location_key))
            assert len((await session.execute(statement, scoped_address_parameters({}))).scalars().all()) == 1
        assert current_network_address_scope() is None and session.in_transaction()
        with pytest.raises(CustomImportReadUnavailableError):
            async with provider_network_read_scope(session, replace(binding, manifest_sha256="f" * 64)):
                pytest.fail("Changed manifest hashes must not enter the provider scope")


async def _copy_selected(fixture, receipt):
    connection = fixture.connection
    schema = "site_copy_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await connection.execute(
            f'CREATE TABLE "{schema}".entity_address_unified '
            f'(LIKE "{fixture.address.schema}".entity_address_unified INCLUDING ALL)'
        )
        await connection.execute(f"""CREATE TABLE "{schema}".provider_location_binding(
            provider_system text,provider_id text,location_id uuid,location_key varchar(64),entity_type text,entity_id text,
            PRIMARY KEY(provider_system,provider_id,location_id))""")
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.executor_role}"')
        await connection.execute(
            f'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA "{schema}" TO "{fixture.executor_role}"'
        )
        connection = fixture.execution_connection
        async with connection.transaction(isolation="repeatable_read"):
            await _cancel_copy(connection, receipt, schema, fixture.control_schema)
            await copy_approved_site_rows(connection, (receipt,), schema, control_schema=fixture.control_schema)
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".entity_address_unified') == 1
            assert await connection.fetchval(
                f'SELECT canonical_network_ids FROM "{schema}".entity_address_unified'
            ) == [42]
            assert (
                await connection.fetchval(
                    f"SELECT encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') "
                    f'FROM "{schema}".entity_address_unified address'
                )
                == receipt.records[0].address_row_sha256
            )
            await copy_approved_site_rows(
                connection, (receipt,), schema, exact_rows=True, control_schema=fixture.control_schema
            )
            await connection.execute(f"UPDATE \"{schema}\".entity_address_unified SET canonical_network_ids='{{}}'")
            await copy_approved_site_rows(
                connection,
                (receipt,),
                schema,
                exact_rows=True,
                reproject_network_ids=True,
                control_schema=fixture.control_schema,
            )
            assert (
                await connection.fetchval(f'SELECT canonical_network_ids FROM "{schema}".entity_address_unified') == []
            )
            damaged = replace(
                receipt,
                compact_address_relations=(
                    ("base", fixture.address.schema, "entity_address_unified", receipt.address_table_oid + 1),
                ),
            )
            with pytest.raises(RetainedSiteAdoptionError, match="receipt differs"):
                await copy_approved_site_rows(connection, (damaged,), schema, control_schema=fixture.control_schema)
    finally:
        connection = fixture.connection
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None


async def _cancel_copy(connection, receipt, schema, control_schema):
    copied, release = asyncio.Event(), asyncio.Event()

    class PausedCopy:
        def __getattr__(self, name):
            return getattr(connection, name)

        async def copy_to_table(self, *arguments, **options):
            status = await connection.copy_to_table(*arguments, **options)
            copied.set()
            await release.wait()
            return status

    operation = asyncio.create_task(
        copy_approved_site_rows(PausedCopy(), (receipt,), schema, control_schema=control_schema)
    )
    try:
        await asyncio.wait_for(copied.wait(), timeout=30)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done()
    finally:
        release.set()
        if not operation.done():
            operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".entity_address_unified') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".provider_location_binding') == 0
    assert connection.is_in_transaction()


async def _refuse_retired(fixture, manifest, receipt):
    await fixture.connection.execute(f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false')
    async with publication._reader(fixture) as (_connection, driver):
        with network_address_read_scope(manifest, (42,)) as scope:
            with pytest.raises(NetworkProviderReadUnavailable):
                await read_network_provider_page(driver, scope, control_schema=fixture.control_schema)
        with pytest.raises(RegistrySourceSiteCatalogUnavailable):
            await read_registry_source_sites(
                driver,
                generation_id=manifest.generation_id,
                provider_system="npi",
                provider_id="1234567893",
                control_schema=fixture.control_schema,
            )
        row = receipt.records[0]
        selection_by_field = {
            name: getattr(row, name)
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        }
        with pytest.raises(RetainedSiteAdoptionError):
            await resolve_retained_site_adoptions(
                driver, manifest, json.dumps([selection_by_field]).encode(), control_schema=fixture.control_schema
            )


@pytest.mark.asyncio
async def test_historical_custom_offices_cannot_reappear_through_old_edges(compact_core_db):
    fixture = compact_core_db
    await fixture.connection.execute(
        f"UPDATE \"{fixture.address.schema}\".entity_address_unified SET row_origin='network_custom'"
    )
    async with publication._reader(fixture) as (_connection, driver):
        manifest = await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema)
        for options in ({"visible_membership_sql": "TRUE"}, {"network_ids": (42,)}):
            relation, arguments = compact_address_native_sql(manifest.compact_layout, first_parameter=1, **options)
            assert await driver.fetchval(f"SELECT count(*) FROM {relation} addresses", *arguments) == 0
