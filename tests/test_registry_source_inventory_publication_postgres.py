# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real admitted CMS inventory reaches custody, publication, scoped reads and pinned replay."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sanic import response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.network_address_scope import (
    canonical_network_read,
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.network_compact_address import compact_address_native_sql
from api.network_provider_only_read import read_network_provider_only_page
from api.network_provider_read import NetworkOfficeSelection, NetworkProviderReadUnavailable, read_network_provider_page
from api.network_read_execution import execute_network_read
from process.network_address_projection import _identifier
from process.network_candidate_custody import verify_network_candidate_read_metadata
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_maintenance import read_sealed_source_inventory_for_maintenance
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_inventory_store import load_source_inventory_for_api, source_inventory_entry
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.cms_registry_recipe_postgres_support import serving_schema as serving_schema
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_initial_source_office_bindings_postgres import cms_full_office_source as cms_full_office_source
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_registry_compact_source_offices_postgres import _rebind
from tests.test_registry_compact_source_offices_postgres import compact_cms_source as compact_cms_source
from tests.test_registry_source_inventory_schema_postgres import _migration

pytestmark = pytest.mark.asyncio
_HEAPS = {
    "network_membership",
    "provider_location_binding",
    "network_membership_source",
    "network_source_contribution",
    "network_source_inventory",
    "network_provider_only_evidence",
    "network_slice_ref",
    "network_office_edge",
}


def _document(value):
    return json.loads(value) if type(value) is str else value


async def _candidate(context, copy_target):
    fixture = context.fixture
    return dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
    )


async def _compose(context, approved_revision, manifest=None):
    request_id = uuid4()
    context.requests.append(request_id)
    copy_target, addresses = await compose_registry_membership_candidate(
        context.fixture.execution_connection,
        request_id=request_id,
        approved_revision=approved_revision,
        expected_head=0 if manifest is None else manifest.generation_id,
        source_manifest=manifest,
        address_sources=RegistryCompositionAddressSources(
            context.source,
            compact=True,
            initial_source_recipes=(RegistrySourceMembershipRecipe(context.source_pin, context.reviewed[4]),)
            if manifest is None
            else (),
        ),
        writer_roles=context.fixture.writer_roles,
        control_schema=context.fixture.control_schema,
    )
    context.targets.append(copy_target)
    assert addresses == context.source
    return copy_target


async def _prepare(context, copy_target):
    return await prepare_network_candidate(
        context.fixture.execution_connection,
        copy_target,
        context.source,
        **context.fixture.writer_roles,
        control_schema=context.fixture.control_schema,
    )


async def _head(context):
    return await context.fixture.connection.fetchval(
        f'SELECT generation_id FROM "{context.fixture.control_schema}".network_serving_control WHERE id=1'
    )


async def _no_seal(context, copy_target, expected_head):
    assert await _head(context) == expected_head
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{context.fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 0
    )


async def _assert_inventory(context, copy_target):
    candidate_by_field = await _candidate(context, copy_target)
    metadata = verify_source_recipe_metadata(candidate_by_field)
    assert metadata.inventory is not None
    root = metadata.inventory.root
    assert (root.entry_count, root.contribution_count, root.derivation_count) == (1, 1, 1)
    rows = await context.fixture.connection.fetch(
        f'SELECT * FROM "{copy_target.schema_name}".network_source_inventory ORDER BY ordinal'
    )
    entries = tuple(source_inventory_entry(native_row) for native_row in rows)
    assert len(entries) == 1 and source_inventory_root(entries) == root
    contribution = verify_registry_source_contributions(candidate_by_field)
    office = verify_initial_source_office_receipt(candidate_by_field)
    assert contribution["revision"] == 3 and contribution["source_count"] == 1
    assert (office["revision"], office["recipe_count"], office["office_rows"]) == (4, 1, 1)
    descriptor = json.loads(entries[0].contribution_json)
    assert (descriptor["membership_rows"], descriptor["provider_only_rows"]) == (1, 3)
    namespace = _identifier(copy_target.schema_name)
    assert (
        await context.fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.network_provider_only_evidence")
        == 3
    )
    assert (
        await context.fixture.connection.fetchval(
            "SELECT to_regclass($1)", copy_target.schema_name + ".entity_address_unified"
        )
        is None
    )
    return descriptor


async def _heap_versions(context, copy_target):
    connection = context.fixture.connection
    versions_by_name = {}
    for name in sorted(_HEAPS):
        relation = _identifier(copy_target.schema_name) + "." + _identifier(name)
        identity = await connection.fetchrow(
            "SELECT oid::bigint,relfilenode::bigint FROM pg_class WHERE oid=to_regclass($1)", relation
        )
        assert identity is not None
        rows = await connection.fetch(
            f"SELECT ctid::text,xmin::text,to_jsonb(heap)::text AS row_json FROM {relation} heap ORDER BY ctid"
        )
        versions_by_name[name] = (dict(identity), [dict(native_row) for native_row in rows])
    return versions_by_name


async def _publish(context, copy_target):
    fixture = context.fixture
    seal = await fixture.seal_candidate(copy_target)
    assert seal["receipt"]["revision"] == 4
    closure = seal["receipt"]["closure"]
    versions = await _heap_versions(context, copy_target)
    assert closure["relation_oids"] == {name: state[0]["oid"] for name, state in versions.items()}
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        publication = await publish_network_candidate(
            fixture.execution_connection, copy_target, control_schema=fixture.control_schema
        )
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(
            fixture.execution_connection, control_schema=fixture.control_schema
        )
    assert manifest.candidate_id == copy_target.candidate_id
    return publication, manifest


async def _replay(context, copy_target, publication):
    fixture = context.fixture
    before = await _heap_versions(context, copy_target)
    statements = []
    fixture.execution_connection.add_query_logger(statements.append)
    try:
        replay = await prepare_and_publish_network_candidate(
            fixture.execution_connection,
            copy_target,
            context.source,
            **fixture.writer_roles,
            control_schema=fixture.control_schema,
        )
        await asyncio.sleep(0)
    finally:
        fixture.execution_connection.remove_query_logger(statements.append)
    assert replay == publication | {"replayed": True}
    forbidden = ("ALTER", "CREATE", "COPY", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "GRANT", "REVOKE", "COMMENT")
    assert not any(
        statement.query.lstrip().upper().startswith(forbidden) or "FOR UPDATE" in statement.query.upper()
        for statement in statements
    )
    assert await _heap_versions(context, copy_target) == before


async def _read_scope(connection, manifest, network_id, control_schema):
    relation, arguments = compact_address_native_sql(
        manifest.compact_layout, first_parameter=1, network_ids=(network_id,)
    )
    selected = await connection.fetch(
        f"SELECT location_key,entity_id,canonical_network_ids FROM {relation} office ORDER BY location_key", *arguments
    )
    with network_address_read_scope(manifest, (network_id,)) as scope:
        offices = await read_network_provider_page(connection, scope, control_schema=control_schema)
        providers = await read_network_provider_only_page(connection, scope, control_schema=control_schema)
        nearby = await read_network_provider_page(
            connection,
            scope,
            control_schema=control_schema,
            office_filters=NetworkOfficeSelection(lat=34.0, long=-118.0, radius_miles=1.0),
        )
        distant = await read_network_provider_page(
            connection,
            scope,
            control_schema=control_schema,
            office_filters=NetworkOfficeSelection(lat=50.0, long=10.0, radius_miles=1.0),
        )
    return {
        "addresses": [dict(native_row) for native_row in selected],
        "offices": offices,
        "providers": providers,
        "nearby": nearby,
        "distant": distant,
    }


async def _canonical_request(context, manifest, copy_target, monkeypatch):
    """Pin real compact custody through the public selector wrapper without an address clone."""
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-read-token")
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", context.fixture.control_schema)
    network_id = context.reviewed[2][0]["record_id"]
    engine = create_async_engine(context.fixture.execution_url)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            statements = []
            with driver.query_logger(statements.append):
                api_manifest = await resolve_network_serving_manifest(
                    driver,
                    generation_id=manifest.generation_id,
                    control_schema=context.fixture.control_schema,
                    api_read=True,
                )
                assert api_manifest == manifest
                metadata = await verify_network_candidate_read_metadata(
                    driver, copy_target, control_schema=context.fixture.control_schema
                )
                candidate_by_field = await _candidate(context, copy_target)
                inventory, entries = await load_source_inventory_for_api(
                    driver, copy_target, candidate_by_field, control_schema=context.fixture.control_schema
                )
                assert inventory.root == source_inventory_root(entries) and len(entries) == 1
                await _assert_canonical_locations(session, manifest, network_id)
                await asyncio.sleep(0)
            assert metadata["seal_sha256"] == manifest._custody_seal_sha256
            assert not any(
                query_event.query.lstrip().upper().startswith("LOCK TABLE")
                or "pg_advisory" in query_event.query.lower()
                for query_event in statements
            )
            request = SimpleNamespace(
                query_string=f"network_ids={network_id}",
                headers={"Authorization": "Bearer synthetic-network-read-token"},
                ctx=SimpleNamespace(sa_session=session),
            )

            read_addresses = _canonical_address_handler(session, manifest)

            with driver.query_logger(statements.append):
                http_response = await read_addresses(request)
                await asyncio.sleep(0)
            assert not any(
                query_event.query.lstrip().upper().startswith("LOCK TABLE")
                or "pg_advisory" in query_event.query.lower()
                for query_event in statements
            )
            assert http_response.status == 200 and json.loads(http_response.body) == {"locations": ["a" * 64]}
            assert http_response.headers["X-Network-Generation"] == str(manifest.generation_id)
            assert http_response.headers["Cache-Control"] == "private, no-store"
            assert current_network_address_scope() is None and session.in_transaction()
    finally:
        await engine.dispose()


async def _maintenance_inventory(context, copy_target):
    async with context.fixture.connection.transaction(isolation="read_committed"):
        candidate_by_field, seal, entries = await read_sealed_source_inventory_for_maintenance(
            context.fixture.connection, copy_target, control_schema=context.fixture.control_schema
        )
        inventory = verify_source_recipe_metadata(candidate_by_field).inventory
        assert candidate_by_field["state"] == "published" and source_inventory_root(entries) == inventory.root
        assert seal["receipt"]["closure"]["relation_oids"]["network_source_inventory"] > 0


def _assert_selected(read_by_kind, network_id):
    assert read_by_kind["addresses"] == [
        {"location_key": "a" * 64, "entity_id": "1000000491", "canonical_network_ids": [network_id]}
    ]
    offices, providers = read_by_kind["offices"], read_by_kind["providers"]
    assert offices["total_count"] == 1 and offices["providers"][0]["provider_id"] == "1000000491"
    assert {location["location_key"] for location in offices["providers"][0]["locations"]} == {"a" * 64}
    assert providers["total_count"] == 2 and {provider["provider_id"] for provider in providers["providers"]} == {
        "1000000491",
        "1000000004",
    }
    assert all("locations" not in provider for provider in providers["providers"])
    assert read_by_kind["nearby"] == offices and read_by_kind["distant"]["total_count"] == 0


@asynccontextmanager
async def _reader(context):
    connection = await asyncpg.connect(
        context.fixture.execution_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            yield connection
    finally:
        await connection.close()


@asynccontextmanager
async def _missing_heap(context, copy_target, name):
    connection = context.fixture.connection
    namespace = _identifier(copy_target.schema_name)
    hidden = name + "_hidden"
    original = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", f"{namespace}.{name}")
    assert original is not None and await connection.fetchval("SELECT to_regclass($1)", f"{namespace}.{hidden}") is None
    await connection.execute(f"ALTER TABLE {namespace}.{_identifier(name)} RENAME TO {_identifier(hidden)}")
    try:
        yield
    finally:
        assert await connection.fetchval("SELECT to_regclass($1)::oid::bigint", f"{namespace}.{hidden}") == original
        await connection.execute(f"ALTER TABLE {namespace}.{_identifier(hidden)} RENAME TO {_identifier(name)}")


@asynccontextmanager
async def _modified_header(context, copy_target):
    candidate_by_field = await _candidate(context, copy_target)
    original = _document(candidate_by_field["source_recipes_json"])
    changed_by_field = {**original, "root": {**original["root"], "entries_sha256": "0" * 64}}
    relation = _identifier(context.fixture.control_schema) + ".network_membership_candidate"
    await context.fixture.connection.execute(
        f"UPDATE {relation} SET source_recipes_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(changed_by_field),
    )
    try:
        yield
    finally:
        await context.fixture.connection.execute(
            f"UPDATE {relation} SET source_recipes_json=$2::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            json.dumps(original),
        )


@asynccontextmanager
async def _wrong_closure_oid(context, copy_target):
    connection = context.fixture.connection
    candidate_by_field = await _candidate(context, copy_target)
    original = _document(candidate_by_field["validation_json"])
    changed_by_field = json.loads(json.dumps(original))
    actual = changed_by_field["writer_closure"]["relation_oids"]["network_source_inventory"]
    wrong = changed_by_field["writer_closure"]["relation_oids"]["network_provider_only_evidence"]
    assert actual != wrong
    changed_by_field["writer_closure"]["relation_oids"]["network_source_inventory"] = wrong
    relation = _identifier(context.fixture.control_schema) + ".network_membership_candidate"
    await connection.execute(
        f"UPDATE {relation} SET validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(changed_by_field),
    )
    try:
        yield
    finally:
        await connection.execute(
            f"UPDATE {relation} SET validation_json=$2::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            json.dumps(original),
        )


async def _refusals(context, copy_target):
    expected_head = await _head(context)
    candidate_before = await _candidate(context, copy_target)
    for name in ("network_source_inventory", "network_provider_only_evidence"):
        async with _missing_heap(context, copy_target, name):
            with pytest.raises(ValueError):
                await context.fixture.seal_candidate(copy_target)
            await _no_seal(context, copy_target, expected_head)
    for probe in (_modified_header, _wrong_closure_oid):
        async with probe(context, copy_target):
            with pytest.raises(ValueError):
                await context.fixture.seal_candidate(copy_target)
            await _no_seal(context, copy_target, expected_head)
    assert await _candidate(context, copy_target) == candidate_before


async def _pinned_publication(context, original):
    fixture = context.fixture
    original_network = context.reviewed[2][0]["record_id"]
    async with _reader(context) as connection:
        pinned = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        expected = await _read_scope(connection, pinned, original_network, fixture.control_schema)
        _assert_selected(expected, original_network)
        revision = await _rebind(context)
        second = await _compose(context, revision, original)
        descriptor = await _assert_inventory(context, second)
        assert descriptor["fact_candidate_id"] == original.candidate_id
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{second.schema_name}".network_source_contribution'
            )
            == 0
        )
        await _prepare(context, second)
        _publication, current = await _publish(context, second)
        assert current.generation_id != original.generation_id
        assert current.compact_layout.base.table_oid == original.compact_layout.base.table_oid
        assert await _read_scope(connection, pinned, original_network, fixture.control_schema) == expected
        assert (
            await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        ).generation_id == original.generation_id
    async with _reader(context) as connection:
        latest = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        _assert_selected(
            await _read_scope(connection, latest, context.reviewed[2][1]["record_id"], fixture.control_schema),
            context.reviewed[2][1]["record_id"],
        )
        with network_address_read_scope(latest, (original_network,)) as scope:
            offices = await read_network_provider_page(connection, scope, control_schema=fixture.control_schema)
            assert offices["total_count"] == 0
            with pytest.raises(NetworkProviderReadUnavailable):
                await read_network_provider_only_page(connection, scope, control_schema=fixture.control_schema)


async def test_inventory_source_publication_and_pinned_replay(compact_cms_source, monkeypatch):
    context = compact_cms_source
    await context.fixture.connection.execute(_migration()._ddl(context.fixture.control_schema))
    base = _identifier(context.source.schema_name) + "." + _identifier(context.source.table_name)
    base_before = await context.fixture.connection.fetch(
        f"SELECT location_key,entity_id,npi,second_line FROM {base} ORDER BY location_key"
    )
    assert {native_row["location_key"] for native_row in base_before if native_row["entity_id"] == "1000000491"} == {
        "a" * 64,
        "b" * 64,
    }
    first = await _compose(context, context.reviewed[5].approved_revision)
    descriptor = await _assert_inventory(context, first)
    assert descriptor["fact_candidate_id"] is None
    prepared = await _prepare(context, first)
    assert prepared["state"] == "ready"
    with pytest.raises(NetworkPublicationError):
        async with context.fixture.execution_connection.transaction(isolation="repeatable_read"):
            await publish_network_candidate(
                context.fixture.execution_connection, first, control_schema=context.fixture.control_schema
            )
    await _no_seal(context, first, None)
    await _refusals(context, first)
    publication, manifest = await _publish(context, first)
    assert manifest.compact_layout.base.table_oid == context.address.descriptor["table"]["oid"]
    assert manifest.compact_layout.custom is None
    await _canonical_request(context, manifest, first, monkeypatch)
    await _maintenance_inventory(context, first)
    await _replay(context, first, publication)
    await _pinned_publication(context, manifest)
    assert (
        await context.fixture.connection.fetch(
            f"SELECT location_key,entity_id,npi,second_line FROM {base} ORDER BY location_key"
        )
        == base_before
    )
    assert (
        await context.fixture.connection.fetchval("SELECT to_regclass($1)::oid::bigint", base)
        == manifest.compact_layout.base.table_oid
    )


async def _assert_canonical_locations(session, manifest, network_id):
    """Consume the same scoped logical office relation under the existing pinned session."""
    with network_address_read_scope(manifest, (network_id,)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        guarded = await execute_network_read(
            session,
            text(f"SELECT location_key FROM {relation} office ORDER BY location_key"),
            scoped_address_parameters({}),
        )
        assert guarded.scalars().all() == ["a" * 64]


def _canonical_address_handler(session, manifest):
    """Build the unchanged public-selector handler around its existing read-only session."""

    @canonical_network_read
    async def read_addresses(_request):
        assert current_network_address_scope().manifest == manifest
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        keys = await execute_network_read(
            session,
            text(f"SELECT location_key FROM {relation} office ORDER BY location_key"),
            scoped_address_parameters({}),
        )
        return response.json({"locations": keys.scalars().all()})

    return read_addresses
