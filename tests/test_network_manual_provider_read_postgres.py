# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read approved non-NPI identities through actual publication and reader ACLs."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from api import network_provider_read as readers
from api.network_address_scope import network_address_read_scope
from api.network_provider_read import (
    NetworkOfficeSelection,
    NetworkProviderNotFound,
    NetworkProviderReadError,
    NetworkProviderReadUnavailable,
    read_network_provider_detail,
    read_network_provider_page,
)
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_candidate_composition_postgres import _remove_candidates, _roles
from tests.test_registry_retained_site_adoption_postgres import _published
from tests.test_registry_source_site_catalog_postgres import catalog_db as catalog_db

pytestmark = pytest.mark.asyncio


async def _compose(fixture, seed, request_id, expected_head):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    target, addresses = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=seed.revision,
        expected_head=expected_head,
        address_sources=RegistryCompositionAddressSources(fixture.base),
        writer_roles=_roles(fixture),
        control_schema=fixture.control_schema,
    )
    fixture.copy_targets.append(target)
    await prepare_and_publish_network_candidate(
        fixture.connection, target, addresses, **_roles(fixture), control_schema=fixture.control_schema
    )
    async with fixture.connection.transaction(readonly=True):
        return await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)


@pytest.fixture
async def manual_read_db(custom_db):
    fixture = custom_db
    fixture.copy_targets = []
    database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    seed = await _seed(fixture)
    request_id = uuid4()
    try:
        source = await _compose(fixture, seed, request_id, 0)
        yield SimpleNamespace(**vars(fixture), seed=seed, source=source)
    finally:
        await _remove_candidates(fixture, fixture.copy_targets, request_id)


async def _reader_grants(fixture):
    reader = '"' + fixture.roles["reader"] + '"'
    namespace = '"' + fixture.control_schema + '"'
    await fixture.connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {reader}")
    for table in ("network_serving_manifest", "network_membership_candidate", "network_serving_control"):
        await fixture.connection.execute(f"GRANT SELECT ON {namespace}.{table} TO {reader}")


async def _read(fixture, *, network_ids=None, detail=None, role=True, source=None, **page):
    office_filters = NetworkOfficeSelection(
        **{field: page.pop(field) for field in ("location_id", "lat", "long", "radius_miles") if field in page}
    )
    if role:
        await _reader_grants(fixture)
    network_ids = network_ids or (fixture.seed.network["record_id"],)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        if role:
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        with network_address_read_scope(source or fixture.source, network_ids) as scope:
            if detail:
                return await read_network_provider_detail(
                    fixture.connection,
                    scope,
                    **detail,
                    **page,
                    office_filters=office_filters,
                    control_schema=fixture.control_schema,
                )
            return await read_network_provider_page(
                fixture.connection, scope, control_schema=fixture.control_schema, office_filters=office_filters, **page
            )


async def test_approved_manual_provider_without_npi_has_exact_selected_office(manual_read_db):
    fixture = manual_read_db
    page = await _read(fixture)
    assert set(page) == {"generation_id", "total_count", "limit", "offset", "has_more", "providers"}
    assert page["generation_id"] == fixture.source.generation_id and page["total_count"] == 1
    assert page["has_more"] is False and len(page["providers"]) == 1
    provider = page["providers"][0]
    assert set(provider) == {"provider_system", "provider_id", "display_name", "npi", "locations"}
    assert provider["provider_system"] == "manual" and provider["provider_id"] == fixture.seed.provider["record_id"]
    assert provider["display_name"] == fixture.seed.provider["record"]["display_name"] and provider["npi"] is None
    assert len(provider["locations"]) == 1
    office = provider["locations"][0]
    assert office["location_id"] == fixture.seed.first["record_id"] and office["second_line"] == "Suite 2"
    assert office["lat"] is None and office["long"] is None
    assert fixture.seed.second["record_id"] not in json.dumps(page)
    assert fixture.source.schema_name not in json.dumps(page) and "canonical_network_ids" not in json.dumps(page)
    detail = await _read(fixture, detail={"provider_system": "manual", "provider_id": provider["provider_id"]})
    assert detail == {"generation_id": page["generation_id"], "provider": provider}
    assert (await _read(fixture, offset=1))["providers"] == []
    assert (await _read(fixture, network_ids=(2147483647,)))["total_count"] == 0
    with pytest.raises(NetworkProviderNotFound):
        await _read(
            fixture,
            network_ids=(2147483647,),
            detail={"provider_system": "manual", "provider_id": provider["provider_id"]},
        )


async def test_manual_exact_office_filter_precedes_count_and_detail(manual_read_db):
    fixture = manual_read_db
    selected_id = fixture.seed.first["record_id"]
    unselected_id = fixture.seed.second["record_id"]
    selected = await _read(fixture, location_id=selected_id)
    assert selected["total_count"] == 1 and len(selected["providers"][0]["locations"]) == 1
    assert selected["providers"][0]["locations"][0]["location_id"] == selected_id
    empty = await _read(fixture, location_id=unselected_id)
    assert empty["total_count"] == 0 and empty["providers"] == [] and not empty["has_more"]
    with pytest.raises(NetworkProviderNotFound):
        await _read(
            fixture,
            detail={"provider_system": "manual", "provider_id": fixture.seed.provider["record_id"]},
            location_id=unselected_id,
        )
    # Missing closed coordinates never fall back to provider-wide or live geocoding.
    geo = await _read(fixture, lat=34, long=-118, radius_miles=1)
    assert geo["total_count"] == 0 and geo["providers"] == []


async def test_spatial_exclusion_cannot_hide_broken_selected_binding(manual_read_db):
    fixture = manual_read_db
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await fixture.connection.execute(
            f"DELETE FROM \"{fixture.source.schema_name}\".provider_location_binding WHERE provider_system='manual'"
        )
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(fixture, role=False, lat=0, long=0, radius_miles=1)
    finally:
        await transaction.rollback()
    assert (await _read(fixture))["total_count"] == 1


async def test_directory_geo_returns_only_selected_near_office(custom_db, monkeypatch):
    from tests import test_registry_retained_site_adoption_postgres as native_source

    fixture = custom_db
    fixture.copy_targets = []
    original = native_source._source_addresses

    async def separated_offices(*args):
        provider_id, source_table = await original(*args)
        await fixture.connection.execute(
            f"UPDATE {source_table} SET lat=CASE WHEN location_key=$1 THEN 34 ELSE 42 END, "
            "long=CASE WHEN location_key=$1 THEN -118 ELSE -71 END",
            "a" * 64,
        )
        return provider_id, source_table

    monkeypatch.setattr(native_source, "_source_addresses", separated_offices)
    try:
        await _published(fixture, 3, "provider_directory", fixture.copy_targets)
        async with fixture.connection.transaction(readonly=True):
            fixture.source = await resolve_network_serving_manifest(
                fixture.connection, control_schema=fixture.control_schema
            )
        network = await fixture.connection.fetchval(
            f'SELECT network_id FROM "{fixture.source.schema_name}".network_membership LIMIT 1'
        )
        fixture.seed = SimpleNamespace(network={"record_id": network})
        complete = await _read(fixture)
        nearby = await _read(fixture, lat=34, long=-118, radius_miles=1)
        provider = nearby["providers"][0]
        assert (
            nearby["total_count"] == 1
            and provider["provider_system"] == "provider_directory"
            and provider["npi"] is None
        )
        assert len(complete["providers"][0]["locations"]) == 3 and len(provider["locations"]) == 1
        assert (provider["locations"][0]["lat"], provider["locations"][0]["long"]) == (34, -118)
        detail = await _read(
            fixture,
            detail={"provider_system": provider["provider_system"], "provider_id": provider["provider_id"]},
            lat=34,
            long=-118,
            radius_miles=1,
        )
        assert detail["provider"] == provider
        assert (await _read(fixture, offset=1, lat=34, long=-118, radius_miles=1))["providers"] == []
        assert (await _read(fixture, lat=0, long=0, radius_miles=1))["total_count"] == 0
    finally:
        await _remove_candidates(fixture, fixture.copy_targets, uuid4())


@pytest.mark.parametrize("catalog_db", [(2, "npi"), (2, "provider_directory")], indirect=True)
async def test_all_identity_namespaces_count_and_page_without_npi_expansion(catalog_db):
    fixture = catalog_db
    network = await fixture.connection.fetchval(
        f'SELECT network_id FROM "{fixture.source.schema_name}".network_membership LIMIT 1'
    )
    fixture.seed = SimpleNamespace(network={"record_id": network})
    complete = await _read(fixture)
    first = await _read(fixture, limit=1)
    second = await _read(fixture, limit=1, offset=1)
    assert complete["total_count"] == 2 and first["has_more"] and not second["has_more"]
    assert first["providers"] + second["providers"] == complete["providers"]
    assert sorted(len(provider["locations"]) for provider in complete["providers"]) == [1, 2]
    detail = await _read(
        fixture, detail={"provider_system": fixture.provider_system, "provider_id": fixture.provider_id}
    )
    assert len(detail["provider"]["locations"]) == 2
    assert all(office["first_line"] == "123 Example Street" for office in detail["provider"]["locations"])
    if fixture.provider_system == "provider_directory":
        assert detail["provider"]["npi"] is None
    with pytest.raises(NetworkProviderNotFound):
        await _read(fixture, detail={"provider_system": "manual", "provider_id": str(uuid4())})


async def test_coordinates_are_read_only_from_the_closed_published_offices(custom_db):
    fixture = custom_db
    fixture.copy_targets = []
    try:
        await fixture.connection.execute(
            f'UPDATE "{fixture.source_schema}".entity_address_unified SET lat=34,long=-118'
        )
        await _published(fixture, 2, "npi", fixture.copy_targets)
        async with fixture.connection.transaction(readonly=True):
            fixture.source = await resolve_network_serving_manifest(
                fixture.connection, control_schema=fixture.control_schema
            )
        network = await fixture.connection.fetchval(
            f'SELECT network_id FROM "{fixture.source.schema_name}".network_membership LIMIT 1'
        )
        fixture.seed = SimpleNamespace(network={"record_id": network})
        page = await _read(fixture)
        assert len(page["providers"]) == 1
        assert [(office["lat"], office["long"]) for office in page["providers"][0]["locations"]] == [
            (34, -118),
            (34, -118),
        ]
    finally:
        await _remove_candidates(fixture, fixture.copy_targets, uuid4())


@pytest.mark.parametrize("catalog_db", [(100, "provider_directory"), (101, "provider_directory")], indirect=True)
async def test_office_bound_and_fixed_bulk_query_count(catalog_db):
    fixture = catalog_db
    network = await fixture.connection.fetchval(
        f'SELECT network_id FROM "{fixture.source.schema_name}".network_membership LIMIT 1'
    )
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            with network_address_read_scope(fixture.source, (network,)) as scope:
                await asyncio.sleep(0)
                queries.clear()
                count = await fixture.connection.fetchval(
                    f'SELECT count(*) FROM "{fixture.source.schema_name}".provider_location_binding WHERE provider_id=$1',
                    fixture.provider_id,
                )
                await asyncio.sleep(0)
                queries.clear()
                await _bounded_office_page(fixture, scope, count)
                await asyncio.sleep(0)
                assert len(queries) == 3
    finally:
        fixture.connection.remove_query_logger(queries.append)


async def _bounded_office_page(fixture, scope, count):
    if count == 101:
        with pytest.raises(NetworkProviderReadUnavailable):
            await read_network_provider_page(fixture.connection, scope, control_schema=fixture.control_schema)
        return
    page = await read_network_provider_page(fixture.connection, scope, control_schema=fixture.control_schema)
    assert max(len(provider["locations"]) for provider in page["providers"]) == 100 and page["total_count"] == 2


async def test_draft_edits_and_new_head_preserve_retained_approved_identity(manual_read_db):
    fixture = manual_read_db
    before = await _read(fixture)
    from tests.test_manual_provider_identity_store_postgres import _create

    correction = replace(
        _create(),
        operation="correct",
        record_id=UUID(fixture.seed.provider["record_id"]),
        expected_revision=1,
        fields={**fixture.seed.provider["record"], "display_name": "Pending name"},
    )
    correction = replace(
        correction,
        fields={key: correction.fields[key] for key in ("display_name", "provider_kind", "aliases", "npi")},
    )
    await _draft(fixture, correction, fixture.seed.actor)
    assert await _read(fixture) == before
    newer = await _compose(fixture, fixture.seed, uuid4(), fixture.source.generation_id)
    assert newer.generation_id != fixture.source.generation_id
    assert await _read(fixture) == before
    current = await _read(fixture, source=newer)
    assert current["providers"] == before["providers"] and newer.approved_custom_revision == fixture.seed.revision


async def test_three_native_reads_and_reader_cannot_modify_candidate(manual_read_db):
    fixture = manual_read_db
    await _reader_grants(fixture)
    statements = []
    fixture.connection.add_query_logger(statements.append)
    try:
        async with fixture.connection.transaction(isolation="serializable", readonly=True):
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
            with network_address_read_scope(fixture.source, (fixture.seed.network["record_id"],)) as scope:
                await asyncio.sleep(0)
                statements.clear()
                result = await read_network_provider_page(
                    fixture.connection, scope, control_schema=fixture.control_schema
                )
                assert result["total_count"] == 1
                await asyncio.sleep(0)
                assert len(statements) == 3 and all(
                    query.query.lstrip().startswith(("SELECT", "WITH")) for query in statements
                )
    finally:
        fixture.connection.remove_query_logger(statements.append)
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await fixture.connection.execute(f'DELETE FROM "{fixture.source.schema_name}".entity_address_unified')


@pytest.mark.parametrize("changes", [{"limit": 0}, {"limit": 101}, {"limit": True}, {"offset": -1}])
async def test_invalid_page_bounds_before_database(manual_read_db, changes):
    fixture = manual_read_db
    with network_address_read_scope(fixture.source, (fixture.seed.network["record_id"],)) as scope:
        with pytest.raises(NetworkProviderReadError):
            await read_network_provider_page(None, scope, **changes)


@pytest.mark.parametrize("identity", [("manual", "not-uuid"), ("npi", "1000000005"), ("unknown", "x")])
async def test_invalid_identity_before_database(identity):
    with pytest.raises(NetworkProviderReadError):
        await read_network_provider_detail(None, None, provider_system=identity[0], provider_id=identity[1])


async def test_transaction_pin_and_output_bounds_fail_closed(manual_read_db, monkeypatch):
    fixture = manual_read_db
    with network_address_read_scope(fixture.source, (fixture.seed.network["record_id"],)) as scope:
        with pytest.raises(NetworkProviderReadError):
            await read_network_provider_page(fixture.connection, scope, control_schema=fixture.control_schema)
        async with fixture.connection.transaction():
            with pytest.raises(NetworkProviderReadError):
                await read_network_provider_page(fixture.connection, scope, control_schema=fixture.control_schema)
    with pytest.raises(NetworkProviderReadUnavailable):
        await _read(fixture, source=replace(fixture.source, manifest_sha256="0" * 64))
    monkeypatch.setattr(readers, "MAX_RESPONSE_BYTES", 1024)
    with pytest.raises(NetworkProviderReadUnavailable):
        await _read(fixture)


@pytest.mark.parametrize(
    "damage",
    [
        "acl",
        "oid",
        "display",
        "coordinates",
        "duplicate_binding",
        "missing_binding",
        "missing_membership",
        "missing_address",
        "missing_scope",
        "foreign_duplicate_binding",
        "wrong_entity_duplicate_binding",
    ],
)
async def test_native_tampering_never_falls_back(manual_read_db, damage):
    fixture = manual_read_db
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        statement, arguments = _tampering_statement(fixture, damage)
        await fixture.connection.execute(statement, *arguments)
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(fixture, role=False)
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(
                fixture,
                role=False,
                detail={"provider_system": "manual", "provider_id": fixture.seed.provider["record_id"]},
            )
    finally:
        await transaction.rollback()
    assert (await _read(fixture))["total_count"] == 1


async def test_unselected_duplicate_binding_does_not_affect_selected_offices(manual_read_db):
    fixture = manual_read_db
    before = await _read(fixture)
    namespace = '"' + fixture.source.schema_name + '"'
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        copied = await fixture.connection.fetchval(
            f"""WITH unselected AS (SELECT * FROM {namespace}.entity_address_unified
              WHERE NOT (canonical_network_ids && ARRAY[$3::integer]) LIMIT 1),
              added AS (INSERT INTO {namespace}.provider_location_binding
              SELECT address.entity_type,address.entity_id,identity,address.location_key,address.entity_type,
                address.entity_id FROM unselected address CROSS JOIN unnest(ARRAY[$1::uuid,$2::uuid]) identity
              RETURNING location_id)
              SELECT count(*) FROM added""",
            uuid4(),
            uuid4(),
            fixture.seed.network["record_id"],
        )
        assert copied == 2
        assert await _read(fixture, role=False) == before
    finally:
        await transaction.rollback()
    assert await _read(fixture) == before


def _tampering_statement(fixture, damage):
    namespace = '"' + fixture.source.schema_name + '"'
    statements_by_damage = {
        "acl": (f'GRANT UPDATE ON {namespace}.entity_address_unified TO "{fixture.roles["reader"]}"', ()),
        "oid": (f"ALTER TABLE {namespace}.entity_address_unified RENAME TO changed_address", ()),
        "display": (f"UPDATE {namespace}.entity_address_unified SET first_line=repeat('x',513)", ()),
        "coordinates": (f"UPDATE {namespace}.entity_address_unified SET lat=91,long=10", ()),
        "missing_membership": (
            f"DELETE FROM {namespace}.network_membership WHERE provider_system='manual'",
            (),
        ),
        "missing_binding": (f"DELETE FROM {namespace}.provider_location_binding WHERE provider_system='manual'", ()),
        "missing_address": (
            f"DELETE FROM {namespace}.entity_address_unified WHERE entity_type='manual'",
            (),
        ),
        "missing_scope": (f"UPDATE {namespace}.entity_address_unified SET canonical_network_ids='{{}}'", ()),
        "foreign_duplicate_binding": (
            f"""INSERT INTO {namespace}.provider_location_binding
              SELECT 'provider_directory','other-provider',$1,location_key,entity_type,entity_id
              FROM {namespace}.provider_location_binding WHERE provider_system='manual' LIMIT 1""",
            (uuid4(),),
        ),
        "wrong_entity_duplicate_binding": (
            f"""INSERT INTO {namespace}.provider_location_binding
              SELECT provider_system,provider_id,$1,location_key,'provider_directory','other-entity'
              FROM {namespace}.provider_location_binding WHERE provider_system='manual' LIMIT 1""",
            (uuid4(),),
        ),
        "duplicate_binding": (
            f"""INSERT INTO {namespace}.provider_location_binding
              SELECT provider_system,provider_id,$1,location_key,entity_type,entity_id
              FROM {namespace}.provider_location_binding WHERE provider_system='manual' LIMIT 1""",
            (uuid4(),),
        ),
    }
    return statements_by_damage[damage]
