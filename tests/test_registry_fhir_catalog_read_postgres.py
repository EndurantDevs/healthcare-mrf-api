# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native metadata query parity, visibility, bounded paging and indexed search."""

import json
import os
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import MAX_CORE_BYTES, table_sql
from process import registry_fhir_catalog_read as catalog
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_registry_fhir_catalog_read import COMPANY, GROUP, binding_document, relationship_document

SECOND_COMPANY = "22222222-2222-4222-8222-222222222222"
SECOND_GROUP = "55555555-5555-4555-8555-555555555555"
GENERATION = "a" * 64


@pytest.fixture
async def catalog_database(monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    metadata, memberships = "fhir_read_metadata_" + suffix, "fhir_read_members_" + suffix
    manifest = SimpleNamespace(
        generation_id=9, manifest_sha256="f" * 64, approved_custom_revision=4, schema_name=memberships
    )
    snapshot_by_field = {
        "metadata_generation": GENERATION,
        "metadata_approved_revision": 6,
        "schema_name": metadata,
        "network_generation": 9,
        "manifest_sha256": "f" * 64,
    }
    fixture = SimpleNamespace(
        connection=connection, metadata=metadata, memberships=memberships, snapshot=snapshot_by_field
    )
    fixture.original_metadata_pin = catalog._metadata_pin
    fixture.manifest = manifest
    monkeypatch.setattr(catalog, "catalog_pin", AsyncMock(return_value=manifest))
    monkeypatch.setattr(catalog, "_metadata_pin", AsyncMock(return_value=snapshot_by_field))
    try:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await connection.execute(f'CREATE SCHEMA "{metadata}"; CREATE SCHEMA "{memberships}"')
        for table in (
            "registry_fhir_catalog_network",
            "registry_fhir_catalog_relationship",
            "registry_fhir_catalog_binding",
        ):
            for statement in table_sql('"' + metadata + '"', table):
                await connection.execute(statement)
        await _membership_tables(fixture)
        await _network(fixture, 7, "Example Network")
        await _network(fixture, 8, "Other Network")
        await _network(fixture, 9, "Unreferenced Network")
        await _binding(fixture, 7, binding_document())
        await _edge(fixture, 7, relationship_document())
        yield fixture
    finally:
        for schema in (metadata, memberships):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        await connection.close()


async def _membership_tables(fixture):
    namespace = '"' + fixture.memberships + '"'
    await fixture.connection.execute(f"""
      CREATE TABLE {namespace}.network_membership(network_id integer,provider_system text,provider_id text,location_id text);
      CREATE TABLE {namespace}.provider_location_binding(provider_system text,provider_id text,location_id text,
        location_key text,entity_type text,entity_id text);
      CREATE TABLE {namespace}.entity_address_unified(location_key text,entity_type text,entity_id text);
      INSERT INTO {namespace}.network_membership VALUES(7,'npi','1234567890','one');
      INSERT INTO {namespace}.provider_location_binding VALUES('npi','1234567890','one','office','npi','1234567890');
      INSERT INTO {namespace}.entity_address_unified VALUES('office','npi','1234567890')""")


async def _network(fixture, network_id, name, aliases=()):
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.metadata}".registry_fhir_catalog_network '
        "(metadata_generation,network_id,display_name,aliases,archived,revision,benefit_codes,search_name) "
        "VALUES($1,$2,$3,$4::jsonb,false,1,NULL,$5)",
        GENERATION,
        network_id,
        name,
        json.dumps(list(aliases)),
        "\n".join((name, *aliases)).lower(),
    )


async def _edge(fixture, network_id, document, *, plan_search=None, company_search=None, binding_id=None):
    company, group = document["company"], document["group"]
    binding_id = binding_id or await _source_binding(fixture, network_id, document["source_id"])
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.metadata}".registry_fhir_catalog_relationship '
        "(metadata_generation,relationship_id,network_id,source_id,company_id,group_id,company_search,plan_search,document_json,source_generation,binding_id) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)",
        GENERATION,
        document["relationship_id"],
        network_id,
        document["source_id"],
        UUID(company["company_id"]) if company else None,
        UUID(group["group_id"]) if group and group["group_id"] else None,
        (company_search or (company["display_name"] if company else "")).lower(),
        (plan_search or (document["plan"]["display_name"] if document["plan"] else "")).lower(),
        json.dumps(document),
        "e" * 64,
        binding_id,
    )


async def _binding(fixture, network_id, document):
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.metadata}".registry_fhir_catalog_binding '
        "(metadata_generation,network_id,binding_id,source_id,document_json) VALUES($1,$2,$3,$4,$5::jsonb)",
        GENERATION,
        network_id,
        UUID(document["binding_id"]),
        document["source_id"],
        json.dumps(document),
    )
    return UUID(document["binding_id"])


async def _source_binding(fixture, network_id, source_id):
    existing = await fixture.connection.fetchval(
        f'SELECT binding_id FROM "{fixture.metadata}".registry_fhir_catalog_binding WHERE network_id=$1 AND source_id=$2',
        network_id,
        source_id,
    )
    if existing is not None:
        return existing
    document = binding_document()
    document["binding_id"] = str(uuid4())
    document["source_id"] = source_id
    return await _binding(fixture, network_id, document)


async def _read(fixture, query=None, policy=None, network_id=None, *, control_schema="sample_control"):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        return await catalog.read_fhir_catalog(
            fixture.connection,
            query or catalog.FHIRCatalogQuery(),
            excluded_network_ids=(),
            organization_policy=policy,
            control_schema=control_schema,
            network_id=network_id,
        )


@pytest.mark.asyncio
async def test_combined_filters_share_relationship(catalog_database):
    fixture = catalog_database
    second = relationship_document()
    second["relationship_id"] = "1" * 64
    second["company"] = {"company_id": SECOND_COMPANY, "display_name": "Other Insurance"}
    second["plan"]["display_name"] = "Silver Plan"
    second["group"]["group_id"] = SECOND_GROUP
    second["source_id"] = "other-fhir"
    await _edge(fixture, 7, second)
    assert (await _read(fixture, catalog.FHIRCatalogQuery(company_id=COMPANY, plan_search="Silver")))["total"] == 0
    assert (await _read(fixture, catalog.FHIRCatalogQuery(company_id=COMPANY, group_id=SECOND_GROUP)))["total"] == 0
    matched = await _read(fixture, catalog.FHIRCatalogQuery(company_id=SECOND_COMPANY, plan_search="Silver"))
    assert matched["total"] == 1 and len(matched["items"]) == 1
    assert matched["items"][0]["matching_relationships"]["items"][0]["company"]["company_id"] == SECOND_COMPANY
    source = RegistryNetworkSourceCoordinates(
        "fhir", "sample-fhir", "sample_source", "capture-one", "registry-retained-normalized-fhir-v1", "edition-one"
    )
    assert (await _read(fixture, catalog.FHIRCatalogQuery(source=source, plan_search="Silver")))["total"] == 0


@pytest.mark.asyncio
async def test_visibility_precedes_counts(catalog_database):
    fixture = catalog_database
    second = relationship_document()
    second["relationship_id"] = "2" * 64
    second["group"]["group_id"] = SECOND_GROUP
    second["group"]["display_name"] = "Other Parent"
    await _edge(fixture, 7, second)
    assert (await _read(fixture))["items"][0]["matching_relationships"]["total"] == 2
    policy = RegistryOrganizationVisibility((), tuple(sorted((GROUP, SECOND_GROUP))))
    hidden = await _read(fixture, policy=policy)
    relationships = hidden["items"][0]["matching_relationships"]
    assert relationships["total"] == 1 and relationships["items"][0]["group"] is None
    assert (await _read(fixture, catalog.FHIRCatalogQuery(group_id=GROUP), policy))["total"] == 0
    policy = RegistryOrganizationVisibility((COMPANY,), ())
    assert (await _read(fixture, catalog.FHIRCatalogQuery(plan_search="Gold"), policy))["total"] == 0
    assert (await _read(fixture, policy=policy))["items"][0]["matching_relationships"]["total"] == 0


@pytest.mark.asyncio
async def test_unattached_catalog_has_no_directory_dependency_and_keeps_visibility(catalog_database, monkeypatch):
    fixture = catalog_database
    fixture.snapshot.update(network_generation=None, manifest_sha256=None)
    pinner = AsyncMock(side_effect=AssertionError("an unattached catalog has no directory parent"))
    monkeypatch.setattr(catalog, "catalog_pin", pinner)
    await fixture.connection.execute(f'DROP SCHEMA "{fixture.memberships}" CASCADE')
    page = await _read(fixture)
    assert page["generation"] is None and page["approved_custom_revision"] is None
    assert page["total"] == 3 and all(network["directory_available"] is False for network in page["items"])
    assert page["metadata_generation"] == GENERATION and page["metadata_approved_revision"] == "6"
    matching = await _read(fixture, catalog.FHIRCatalogQuery(company_id=COMPANY, plan_search="Gold"))
    assert [network["network_id"] for network in matching["items"]] == [7]
    hidden = await _read(
        fixture,
        catalog.FHIRCatalogQuery(company_id=COMPANY, plan_search="Gold"),
        RegistryOrganizationVisibility((COMPANY,), ()),
    )
    assert hidden["total"] == 0
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await _read(fixture, catalog.FHIRCatalogQuery(generation_id=9))
    pinner.assert_not_awaited()


@pytest.mark.parametrize(
    "transaction", [None, {"isolation": "read_committed", "readonly": True}, {"isolation": "repeatable_read"}]
)
@pytest.mark.asyncio
async def test_unattached_read_refuses_unpinned_transactions(catalog_database, transaction):
    fixture = catalog_database
    fixture.snapshot.update(network_generation=None, manifest_sha256=None)
    if transaction is None:
        with pytest.raises(catalog.RegistryNetworkCatalogError, match="transaction_required"):
            await catalog.read_fhir_catalog(fixture.connection, catalog.FHIRCatalogQuery(), excluded_network_ids=())
    else:
        async with fixture.connection.transaction(**transaction):
            with pytest.raises(catalog.RegistryNetworkCatalogError, match="transaction_required"):
                await catalog.read_fhir_catalog(fixture.connection, catalog.FHIRCatalogQuery(), excluded_network_ids=())
    catalog.catalog_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_unattached_continuations_pin_metadata_scope_and_fresh_visibility(catalog_database, monkeypatch):
    fixture = catalog_database
    fixture.snapshot.update(network_generation=None, manifest_sha256=None)
    pinner = AsyncMock(side_effect=AssertionError("directory pin is not part of metadata continuation"))
    monkeypatch.setattr(catalog, "catalog_pin", pinner)
    first = await _read(fixture, catalog.FHIRCatalogQuery(limit=1))
    query = catalog.FHIRCatalogQuery(
        limit=1,
        offset=first["next_offset"],
        metadata_generation=GENERATION,
        continuation_scope_sha256=first["continuation_scope_sha256"],
    )
    second = await _read(fixture, query)
    assert {network["network_id"] for network in first["items"]}.isdisjoint(
        network["network_id"] for network in second["items"]
    )
    for changed in (
        replace(query, continuation_scope_sha256="f" * 64),
        replace(query, plan_search="Gold"),
        replace(query, generation_id=9),
    ):
        with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
            await _read(fixture, changed)
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await _read(fixture, query, RegistryOrganizationVisibility((COMPANY,), ()))
    pinner.assert_not_awaited()


@pytest.mark.asyncio
async def test_unattached_read_pins_published_metadata_instead_of_current_directory_head(catalog_database, monkeypatch):
    fixture = catalog_database
    verifier, namespace = await _prepare_metadata_pin_fixture(fixture, monkeypatch)
    await fixture.connection.execute(
        f"UPDATE {namespace}.registry_fhir_catalog_snapshot "
        "SET network_generation=NULL,manifest_sha256=NULL,published_at=clock_timestamp()"
    )
    pinner = AsyncMock(side_effect=AssertionError("the current directory head is not a metadata attachment"))
    monkeypatch.setattr(catalog, "catalog_pin", pinner)
    monkeypatch.setattr(catalog, "_metadata_pin", fixture.original_metadata_pin)
    page = await _read(fixture, control_schema=fixture.metadata)
    assert page["metadata_generation"] == GENERATION and page["metadata_approved_revision"] == "6"
    assert page["generation"] is None and page["approved_custom_revision"] is None
    assert all(network["directory_available"] is False for network in page["items"])
    verifier.assert_awaited_once()
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await _read(fixture, catalog.FHIRCatalogQuery(generation_id=10), control_schema=fixture.metadata)
    pinner.assert_not_awaited()


@pytest.mark.asyncio
async def test_literal_alias_unicode_and_last_good(catalog_database):
    fixture = catalog_database
    document = relationship_document()
    document["relationship_id"] = "3" * 64
    await _edge(fixture, 8, document, plan_search="Česká 100%_Gold", company_search="Example Insurance\nOld Insurer")
    query = catalog.FHIRCatalogQuery(plan_search="100%_", company_search="OLD INSURER")
    page = await _read(fixture, query)
    assert [network["network_id"] for network in page["items"]] == [8]
    assert (await _read(fixture, catalog.FHIRCatalogQuery(plan_search="ČESKÁ")))["total"] == 1
    await fixture.connection.execute(
        f'UPDATE "{fixture.metadata}".registry_fhir_catalog_binding '
        "SET document_json=jsonb_set(document_json,'{status}','\"last_good\"')"
    )
    retained = await _read(fixture, network_id=7)
    assert retained["items"][0]["directory_available"] is True
    assert retained["items"][0]["source_bindings"]["items"][0]["status"] == "last_good"
    assert (await _read(fixture))["total"] == 3


@pytest.mark.asyncio
async def test_nested_and_byte_continuations(catalog_database):
    fixture = catalog_database
    for ordinal in range(6):
        document = relationship_document()
        document["relationship_id"] = f"{ordinal + 16:064x}"
        document["plan"]["resource_id"] = "plan-" + str(ordinal)
        await _edge(fixture, 7, document)
    page = await _read(fixture, network_id=7)
    assert page["items"][0]["matching_relationships"]["total"] == 7
    assert page["items"][0]["matching_relationships"]["next_offset"] == 5
    query = catalog.FHIRCatalogQuery(
        limit=1,
        generation_id=9,
        metadata_generation=GENERATION,
        relationship_offset=5,
        continuation_scope_sha256=page["continuation_scope_sha256"],
    )
    continued = await _read(fixture, query, network_id=7)
    assert await _read(fixture, replace(query, generation_id=None), network_id=7) == continued
    assert len(continued["items"][0]["matching_relationships"]["items"]) == 2
    assert continued["items"][0]["matching_relationships"]["next_offset"] is None
    for ordinal in range(30):
        aliases = tuple(f"Alias-{index:03d}-" + "x" * 490 for index in range(100))
        await _network(fixture, ordinal + 100, f"Fanout {ordinal:03d}", aliases)
    page = await _read(fixture, catalog.FHIRCatalogQuery(limit=100))
    assert 0 < len(page["items"]) < page["total"] and page["next_offset"] == len(page["items"])
    assert len(json.dumps(page).encode()) <= MAX_CORE_BYTES
    query = catalog.FHIRCatalogQuery(
        limit=100,
        offset=page["next_offset"],
        generation_id=9,
        metadata_generation=GENERATION,
        continuation_scope_sha256=page["continuation_scope_sha256"],
    )
    continued = await _read(fixture, query)
    assert not {network["network_id"] for network in page["items"]} & {
        network["network_id"] for network in continued["items"]
    }


def _plan_names(document):
    names = []
    for entry in document:
        if isinstance(entry, dict):
            if "Index Name" in entry:
                names.append(entry["Index Name"])
            names.extend(_plan_names(entry.values()))
        elif isinstance(entry, list):
            names.extend(_plan_names(entry))
    return names


@pytest.mark.asyncio
async def test_representative_query_uses_trigram(catalog_database):
    fixture = catalog_database
    binding_id = await _source_binding(fixture, 8, "sample-fhir")
    await fixture.connection.execute(
        f"""INSERT INTO "{fixture.metadata}".registry_fhir_catalog_relationship
      (metadata_generation,relationship_id,network_id,source_id,company_id,group_id,company_search,plan_search,document_json,
        source_generation,binding_id)
      SELECT $1,encode(sha256(convert_to(ordinal::text,'UTF8')),'hex'),8,'sample-fhir',NULL,NULL,'',
        CASE WHEN ordinal=12345 THEN 'needle precise plan' ELSE 'ordinary plan '||ordinal END,$2::jsonb,$3,$4
      FROM generate_series(1,30000) ordinal""",
        GENERATION,
        json.dumps(relationship_document()),
        "e" * 64,
        binding_id,
    )
    await fixture.connection.execute(f'ANALYZE "{fixture.metadata}".registry_fhir_catalog_relationship')
    query = catalog.FHIRCatalogQuery(plan_search="needle precise")
    parameters = catalog._query_parameters(query, GENERATION, (), RegistryOrganizationVisibility(), None)
    explained = await fixture.connection.fetchval(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "
        + catalog._catalog_page_sql({"schema_name": fixture.metadata}, fixture.manifest),
        *parameters,
    )
    plan = json.loads(explained)
    assert "registry_fhir_catalog_plan_trgm" in _plan_names(plan)


@pytest.mark.asyncio
async def test_source_filter_binds_exact_capture(catalog_database):
    fixture = catalog_database
    binding = binding_document()
    binding["binding_id"] = str(uuid4())
    binding["dataset_id"] = "capture-two"
    binding["edition_id"] = "edition-two"
    binding_id = await _binding(fixture, 7, binding)
    relationship = relationship_document()
    relationship["relationship_id"] = "4" * 64
    relationship["plan"]["display_name"] = "Silver Plan"
    await _edge(fixture, 7, relationship, binding_id=binding_id)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", "sample-fhir", "sample_source", "capture-one", "registry-retained-normalized-fhir-v1", "edition-one"
    )
    assert (await _read(fixture, catalog.FHIRCatalogQuery(source=coordinates, plan_search="Silver")))["total"] == 0
    coordinates = replace(coordinates, dataset_id="capture-two", edition_id="edition-two")
    assert (await _read(fixture, catalog.FHIRCatalogQuery(source=coordinates, plan_search="Silver")))["total"] == 1


@pytest.mark.asyncio
async def test_detail_preserves_archived_filter(catalog_database):
    fixture = catalog_database
    await fixture.connection.execute(
        f'UPDATE "{fixture.metadata}".registry_fhir_catalog_network SET archived=true WHERE network_id=7'
    )
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await _read(fixture, network_id=7)
    archived = await _read(fixture, catalog.FHIRCatalogQuery(archived=True), network_id=7)
    assert archived["limit"] == 1 and archived["items"][0]["archived"] is True
    unrestricted = await _read(fixture, catalog.FHIRCatalogQuery(archived=None), network_id=7)
    assert unrestricted["items"][0]["network_id"] == 7


async def _prepare_metadata_pin_fixture(fixture, monkeypatch):
    """Install an unpublished descriptor and its snapshot verifier for pin-state assertions."""
    from process import registry_fhir_catalog_custody as custody

    verifier = AsyncMock()
    monkeypatch.setattr(custody, "verify_fhir_catalog_snapshot", verifier)
    namespace = '"' + fixture.metadata + '"'
    await fixture.connection.execute(f"""CREATE TABLE {namespace}.network_serving_control
      (id integer PRIMARY KEY,metadata_generation text,generation_id bigint);
      CREATE TABLE {namespace}.registry_fhir_catalog_snapshot(metadata_generation text,network_generation bigint,
        manifest_sha256 text,metadata_approved_revision bigint,schema_name text,custody_json jsonb,published_at timestamptz,
        membership_mapping_sha256 text,mapping_sha256 text,standing_grant_sha256 text,source_vector_json jsonb,
        projector_version text,accounting_json jsonb,as_of date);
      INSERT INTO {namespace}.network_serving_control VALUES(1,'{"a" * 64}',10)""")
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_snapshot "
        "(metadata_generation,network_generation,manifest_sha256,metadata_approved_revision,schema_name,custody_json,published_at) "
        "VALUES($1,9,$2,6,$3,'{}'::jsonb,NULL)",
        GENERATION,
        "f" * 64,
        fixture.metadata,
    )
    return verifier, namespace


@pytest.mark.asyncio
async def test_unpublished_and_changed_pin_refuse(catalog_database, monkeypatch):
    """Reject unpublished or ineligible pins while retaining valid published metadata generations."""
    fixture = catalog_database
    verifier, namespace = await _prepare_metadata_pin_fixture(fixture, monkeypatch)
    query = catalog.FHIRCatalogQuery()
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await fixture.original_metadata_pin(fixture.connection, query, namespace)
    verifier.assert_not_awaited()
    await fixture.connection.execute(
        f"UPDATE {namespace}.registry_fhir_catalog_snapshot SET published_at=clock_timestamp()"
    )
    pinned = await fixture.original_metadata_pin(fixture.connection, query, namespace)
    assert pinned["metadata_generation"] == GENERATION
    verifier.assert_awaited_once()
    monkeypatch.setattr(catalog, "_metadata_pin", fixture.original_metadata_pin)
    pinner = AsyncMock(return_value=fixture.manifest)
    monkeypatch.setattr(catalog, "catalog_pin", pinner)

    page = await _read(fixture, control_schema=fixture.metadata)
    assert page["generation"] == "9"
    pinner.assert_awaited_with(fixture.connection, 9, fixture.metadata)
    hidden = await _read(
        fixture,
        catalog.FHIRCatalogQuery(company_id=COMPANY),
        policy=RegistryOrganizationVisibility(excluded_company_ids=(COMPANY,)),
        control_schema=fixture.metadata,
    )
    assert hidden["total"] == 0
    query = catalog.FHIRCatalogQuery(generation_id=10)
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await fixture.original_metadata_pin(fixture.connection, query, namespace)
    await fixture.connection.execute(f"UPDATE {namespace}.network_serving_control SET metadata_generation=$1", "c" * 64)
    query = catalog.FHIRCatalogQuery(generation_id=9, metadata_generation=GENERATION)
    retained = await fixture.original_metadata_pin(fixture.connection, query, namespace)
    assert retained["metadata_generation"] == GENERATION
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_snapshot "
        "(metadata_generation,network_generation,manifest_sha256,metadata_approved_revision,schema_name,custody_json,published_at) "
        "VALUES($1,10,$2,6,$3,'{}'::jsonb,clock_timestamp())",
        "c" * 64,
        "e" * 64,
        fixture.metadata,
    )
    successor = SimpleNamespace(**{**vars(fixture.manifest), "generation_id": 10, "manifest_sha256": "e" * 64})

    async def pin(_connection, generation, _schema):
        if generation != 10:
            raise ValueError("retained parent is ineligible")
        return successor

    pinner.side_effect = pin
    assert (await _read(fixture, control_schema=fixture.metadata))["generation"] == "10"
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="unavailable"):
        await _read(fixture, query, control_schema=fixture.metadata)
