# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct catalog sync reads a named endpoint's current header through its edition pointer."""

import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import registry_fhir_catalog_observed_source as observed
from process import registry_fhir_catalog_sources as sources
from process.provider_directory_named_edition import POINTER_SCHEMA, POINTER_TABLE

pytestmark = pytest.mark.asyncio

_HEADER_COLUMNS = (
    "dataset_id text PRIMARY KEY,endpoint_id text,dataset_hash text,status text,is_current boolean,"
    "published_at timestamptz,resource_count bigint,previous_dataset_id text,acquisition_root_run_id text,"
    "publication_metadata_json jsonb"
)


@pytest.fixture
async def catalog_db():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    catalog, edition = "fc_named_catalog_" + suffix, "fc_named_edition_" + suffix
    has_pointer_schema = await connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", POINTER_SCHEMA)
    try:
        for schema in (catalog, edition):
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(f'CREATE TABLE "{schema}".provider_directory_endpoint_dataset({_HEADER_COLUMNS})')
        await connection.execute(
            f'CREATE TABLE "{catalog}".provider_directory_api_endpoint(endpoint_id text PRIMARY KEY,canonical_api_base text)'
        )
        await connection.execute(
            f'CREATE TABLE "{catalog}".provider_directory_source(source_id text PRIMARY KEY,endpoint_id text)'
        )
        await connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{POINTER_SCHEMA}"')
        await connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{POINTER_SCHEMA}"."{POINTER_TABLE}"(catalog_schema text,endpoint_id text,'
            "dataset_id text,schema_name text,previous_dataset_id text,previous_schema_name text,"
            "PRIMARY KEY(catalog_schema,endpoint_id))"
        )
        yield SimpleNamespace(
            connection=connection,
            catalog=catalog,
            edition=edition,
            profile=sources.FHIRCatalogSourceReadProfile(catalog, catalog),
        )
    finally:
        await connection.execute(f'DELETE FROM "{POINTER_SCHEMA}"."{POINTER_TABLE}" WHERE catalog_schema=$1', catalog)
        if not has_pointer_schema:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{POINTER_SCHEMA}" CASCADE')
        for schema in (catalog, edition):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def _register(fixture, endpoint_id):
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_api_endpoint VALUES($1,$2)',
        endpoint_id,
        "https://example.test/fhir",
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_source VALUES($1,$2)', endpoint_id + "-alias", endpoint_id
    )


async def _header(fixture, schema, endpoint_id, dataset_id):
    await fixture.connection.execute(
        f'INSERT INTO "{schema}".provider_directory_endpoint_dataset VALUES'
        "($1,$2,repeat('a',64),'published',true,$3,1,NULL,'root-run',$4::jsonb)",
        dataset_id,
        endpoint_id,
        datetime(2026, 1, 1, tzinfo=UTC),
        json.dumps({"resource_hash_contract": None}),
    )


async def _point(fixture, endpoint_id, dataset_id):
    await fixture.connection.execute(
        f'INSERT INTO "{POINTER_SCHEMA}"."{POINTER_TABLE}" VALUES($1,$2,$3,$4,NULL,NULL)',
        fixture.catalog,
        endpoint_id,
        dataset_id,
        fixture.edition,
    )


async def _inventory(fixture, monkeypatch):
    observed_by_endpoint = {}

    async def accept(connection, profile, *, endpoint_id, dataset_id):
        observed_by_endpoint[endpoint_id] = dataset_id
        return sources.FHIRCatalogSourceDisposition(
            endpoint_id, dataset_id, (endpoint_id + "-alias",), 1, "accepted", None, None, None
        )

    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", accept)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        pages = [page async for page in sources.iter_fhir_catalog_sources(fixture.connection, fixture.profile)]
    return {disposition.endpoint_id: disposition for page in pages for disposition in page}, observed_by_endpoint


async def test_direct_sync_finds_a_named_endpoint_through_its_edition_pointer(catalog_db, monkeypatch):
    fixture = catalog_db
    await _register(fixture, "named-endpoint")
    await _header(fixture, fixture.edition, "named-endpoint", "named-dataset")
    await _point(fixture, "named-endpoint", "named-dataset")
    dispositions, observed_by_endpoint = await _inventory(fixture, monkeypatch)
    assert dispositions["named-endpoint"].status == "accepted"
    assert observed_by_endpoint == {"named-endpoint": "named-dataset"}
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        schema = await sources.fhir_catalog_header_schema(
            fixture.connection, fixture.profile, "named-endpoint", "named-dataset"
        )
        header = await observed._header(
            fixture.connection, fixture.profile, "named-endpoint", "named-dataset", True, schema
        )
    assert schema == fixture.edition and header["dataset_id"] == "named-dataset" and header["current_count"] == 1


async def test_direct_sync_keeps_reading_a_legacy_endpoint_from_the_catalog(catalog_db, monkeypatch):
    fixture = catalog_db
    await _register(fixture, "legacy-endpoint")
    await _header(fixture, fixture.catalog, "legacy-endpoint", "legacy-dataset")
    dispositions, observed_by_endpoint = await _inventory(fixture, monkeypatch)
    assert dispositions["legacy-endpoint"].status == "accepted"
    assert observed_by_endpoint == {"legacy-endpoint": "legacy-dataset"}
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await sources.fhir_catalog_header_schema(
                fixture.connection, fixture.profile, "legacy-endpoint", "legacy-dataset"
            )
            == fixture.catalog
        )


async def test_pointer_to_an_edition_without_its_header_is_refused_not_replaced(catalog_db, monkeypatch):
    fixture = catalog_db
    await _register(fixture, "orphan-endpoint")
    # A catalog header for the same endpoint must not stand in for the pointed edition.
    await _header(fixture, fixture.catalog, "orphan-endpoint", "catalog-dataset")
    await _point(fixture, "orphan-endpoint", "pointed-dataset")
    dispositions, observed_by_endpoint = await _inventory(fixture, monkeypatch)
    assert dispositions["orphan-endpoint"].status == "unavailable"
    assert dispositions["orphan-endpoint"].reason == "named_edition_header_missing"
    assert dispositions["orphan-endpoint"].dataset_id == "pointed-dataset" and observed_by_endpoint == {}
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(sources.FHIRCatalogSourceError, match="named_edition_header_missing"):
            await sources.fhir_catalog_header_schema(
                fixture.connection, fixture.profile, "orphan-endpoint", "pointed-dataset"
            )
