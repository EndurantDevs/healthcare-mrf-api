# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected provider-plan HTTP reads through ordinary native reader sessions."""

import json
import secrets
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import network_catalog as endpoint
from process import reference_family_archive as archive
from process.network_address_projection import PinnedAddressSource
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_record_store import RegistryRecordCommand
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests import test_registry_provider_plan_read_postgres as source_fixtures
from tests.test_network_catalog_routes import v2_envelope
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_initial_source_office_bindings_postgres import _prepare_addresses
from tests.test_network_legacy_membership_source_postgres import _aca_binding, _write_aca_bindings
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command
from tests.test_registry_candidate_composition_postgres import (
    _approve_composition_intent,
    _approve_composition_source_offices,
    _compose_intent_candidate,
)
from tests.test_registry_provider_plan_read_postgres import (
    _raw_offices,
)
from tests.test_registry_provider_plan_read_postgres import (
    provider_plans_db as provider_plans_db,
)

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/registry/serving/providers/plans"
_AUTH = {"Authorization": "Bearer synthetic-provider-plan-control"}


@pytest.fixture
async def transport_db(provider_plans_db, monkeypatch):
    """Authenticate directly as the disposable Reader; retain real endpoint ownership."""
    fixture, source_pin, networks, copy_target = provider_plans_db
    password = secrets.token_urlsafe(24)
    role = fixture.roles["reader"]
    await fixture.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    engine = create_async_engine(fixture.engine.url.set(username=role, password=password), pool_size=1, max_overflow=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    starts, snapshots, requests = [], [], []

    @event.listens_for(engine.sync_engine, "begin")
    def record_begin(connection):
        starts.append(id(connection))

    @event.listens_for(engine.sync_engine, "after_cursor_execute")
    def record_native_scope(connection, cursor, statement, parameters, context, executemany):
        if statement == "SET LOCAL lock_timeout='1s'":
            cursor.execute(
                "SELECT current_user,current_setting('transaction_read_only'),"
                "current_setting('transaction_isolation'),pg_backend_pid(),"
                "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
            )
            snapshots.append(cursor.fetchone())

    app = Sanic("native_provider_plans_" + uuid4().hex)
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    endpoint.register_network_catalog_private_responses(app)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-provider-plan-control")
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)

    @app.middleware("request")
    async def attach_session(request):
        request.ctx.sa_session = sessions()
        requests.append((request.method, request.path, request.ctx.sa_session))

    @app.middleware("response")
    async def close_session(request, response):
        await request.ctx.sa_session.close()

    try:
        yield SimpleNamespace(
            app=app,
            fixture=fixture,
            source_pin=source_pin,
            networks=networks,
            copy_target=copy_target,
            starts=starts,
            snapshots=snapshots,
            requests=requests,
        )
    finally:
        for method, path, session in requests:
            await session.close()
        await engine.dispose()


async def _post(context, document, *, headers=None):
    before = len(context.starts)
    _, reply = await context.app.asgi_client.post(
        _PATH, data=json.dumps(document).encode(), headers=_AUTH if headers is None else headers
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert all(method == "POST" and path == _PATH for method, path, session in context.requests)
    assert all(not session.in_transaction() for method, path, session in context.requests)
    return reply, len(context.starts) - before


def _success_headers(reply, document):
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert reply.headers["X-Network-Generation"] == reply.json["generation"]
    assert reply.json["client_id"] == document["client_id"]
    assert reply.json["policy_revision"] == document["policy_revision"]


async def test_native_protected_post_default_offices_all_hidden_and_unknown_provider(transport_db):
    context = transport_db
    document = v2_envelope({"npi": "1000000491"}, excluded_network_ids=[context.networks[1]["record_id"]])
    reply, transactions = await _post(context, document)
    assert reply.status == 200 and transactions == 1
    _success_headers(reply, document)
    assert reply.json["plan_data"] == reply.json["issuer_data"] == []
    assert len(reply.json["npi_data"]) == 1
    row = reply.json["npi_data"][0]
    assert row["npi_info"]["checksum_network"] == 42 and row["npi_info"]["addresses"] == _raw_offices()[:1]
    assert row["issuer_info"]["issuer_name"] == "Retained issuer"
    all_hidden = v2_envelope(
        {"npi": "1000000491"}, excluded_network_ids=sorted(network["record_id"] for network in context.networks)
    )
    hidden, transactions = await _post(context, all_hidden)
    assert hidden.status == 200 and transactions == 1 and hidden.json["npi_data"] == []
    _success_headers(hidden, all_hidden)
    unknown, transactions = await _post(context, v2_envelope({"npi": "0000000000"}))
    assert unknown.status == 404 and transactions == 1 and "npi_data" not in unknown.json
    assert "X-Network-Generation" not in unknown.headers
    assert len(context.snapshots) == 3
    assert all(
        snapshot[0] == context.fixture.roles["reader"]
        and snapshot[1:3] == ("on", "repeatable read")
        and snapshot[4] is False
        for snapshot in context.snapshots
    )
    assert len({snapshot[3] for snapshot in context.snapshots}) == 1


async def test_native_protected_post_refusal_has_no_unscoped_fallback(transport_db):
    context = transport_db
    document = v2_envelope({"npi": "1000000491"})
    denied, transactions = await _post(context, document, headers={"Authorization": "Bearer wrong"})
    assert denied.status == 403 and transactions == 0 and context.snapshots == []
    invalid, transactions = await _post(context, document | {"access_scope_sha256": "a" * 64})
    assert invalid.status == 400 and transactions == 0 and context.snapshots == []
    injected, transactions = await _post(context, v2_envelope({"npi": "1000000491", "network_ids": [1]}))
    assert injected.status == 400 and transactions == 0
    stale_document = v2_envelope({"npi": "1000000491", "network_generation": "9223372036854775807"})
    stale, transactions = await _post(context, stale_document)
    assert stale.status == 503 and transactions == 1 and "npi_data" not in stale.json
    assert stale.headers["X-Network-Access-Scope"] == stale_document["access_scope_sha256"]
    assert "X-Network-Generation" not in stale.headers
    assert len(context.snapshots) == 1


async def _remove_source_office(context):
    """Publish actual approved removal while retaining independent source membership."""
    fixture = context.fixture
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        original = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    members = await fixture.connection.fetch(
        f'SELECT * FROM "{context.copy_target.schema_name}".network_membership ORDER BY network_id'
    )
    actor = _actor()
    await _approve_composition_source_offices(fixture, original, context.copy_target, members, actor)
    member = members[0]
    command = RegistryRecordCommand(
        "membership",
        member["network_id"],
        "create",
        0,
        {
            "memberships_json": [
                {
                    "network_id": member["network_id"],
                    "provider_system": "npi",
                    "provider_id": "1000000491",
                    "location_id": str(member["location_id"]),
                    "evidence_id": "explicit-removal",
                    "operation": "remove",
                }
            ]
        },
        "Review exact office removal",
        uuid4().hex,
    )
    _, revision = await _approve_composition_intent(fixture, actor, command)
    current, effective, retained = await _compose_intent_candidate(
        (fixture, fixture.extra_targets, fixture.extra_requests, member["network_id"]),
        revision,
        original,
        original.generation_id,
    )
    assert not effective and len(retained) == 1
    return original, current


async def test_native_protected_post_current_policy_and_retained_composition_share_exact_source(transport_db):
    context = transport_db
    original, current = await _remove_source_office(context)
    excluded_ids = [context.networks[1]["record_id"]]
    document = v2_envelope({"npi": "1000000491"}, excluded_network_ids=excluded_ids, policy_revision="7")
    now, transactions = await _post(context, document)
    assert now.status == 200 and transactions == 1 and now.json["npi_data"] == []
    assert now.json["generation"] == str(current.generation_id)
    _success_headers(now, document)
    retained_document = v2_envelope(
        {"npi": "1000000491", "network_generation": str(original.generation_id)},
        excluded_network_ids=excluded_ids,
        policy_revision="7",
    )
    retained, transactions = await _post(context, retained_document)
    assert retained.status == 200 and transactions == 1
    assert retained.json["generation"] == str(original.generation_id)
    assert retained.json["npi_data"][0]["npi_info"]["addresses"] == _raw_offices()[:1]
    _success_headers(retained, retained_document)
    assert retained.headers["X-Network-Access-Scope"] == now.headers["X-Network-Access-Scope"]


async def _canonical_source_inputs(context, monkeypatch, office_b):
    """Use actual native canonical output before the source manifest is sealed."""
    import ptg2_address_canon

    address = (
        office_b["address"],
        office_b.get("address_2"),
        office_b["city"],
        office_b["state"],
        office_b["zip"],
        office_b["country"],
    )
    canonical = ptg2_address_canon.canonicalize_batch([address])[0]
    payload = canonical | {
        "identity_version": ptg2_address_canon.canon_version()["identity_version"],
        "precision": "street",
        "source_bits": 16,
        "merged_into": None,
    }
    original = source_fixtures._insert_reviewed_lineage

    async def load_canonical(session, namespace):
        await original(session, namespace)
        await session.execute(
            text(f'INSERT INTO "{namespace}".mrf_canonical_address VALUES(:key,CAST(:payload AS jsonb))'),
            {"key": UUID(canonical["address_key"]), "payload": json.dumps(payload)},
        )

    monkeypatch.setattr(source_fixtures, "_insert_reviewed_lineage", load_canonical)


async def _next_physical_addresses(context, owner, role, schema):
    """Create a separate full native address source without altering S0 heaps."""

    import ptg2_address_canon

    raw_addresses = [
        (office["address"], office.get("address_2"), office["city"], office["state"], office["zip"], office["country"])
        for office in _raw_offices()
    ]
    canonical = ptg2_address_canon.canonicalize_batch(raw_addresses)
    setup = SimpleNamespace(
        connection=context.fixture.connection,
        engine=context.fixture.engine,
        address_schema=schema,
        owner=owner,
        reader=context.fixture.roles["reader"],
        raw=raw_addresses,
        canonical=canonical,
        sites=[UUID(entry["address_key"]) for entry in canonical],
    )
    await _prepare_addresses(setup)
    await setup.connection.execute(
        f'UPDATE "{schema}".entity_address_unified SET archive_identity_version=$1,row_origin=$2',
        "v" + str(ptg2_address_canon.canon_version()["identity_version"]),
        "base",
    )
    # Reuse only an exact already-published provider/site identity, never a label crosswalk.
    status = await setup.connection.execute(
        f'UPDATE "{schema}".entity_address_unified address SET location_key=binding.location_key '
        f'FROM "{context.copy_target.schema_name}".provider_location_binding binding '
        "WHERE binding.provider_system='npi' AND binding.provider_id=address.entity_id "
        "AND binding.location_id=address.address_key AND binding.entity_type=address.entity_type"
    )
    assert status == "UPDATE 2"
    generation = await setup.connection.fetchval(
        f"SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(address) ORDER BY location_key)::text,'UTF8')),'hex') "
        f'FROM "{schema}".entity_address_unified address'
    )
    return PinnedAddressSource(schema, "entity_address_unified", generation)


async def _admit_next_source(context, monkeypatch, coordinates, owner, role):
    """Prepare genuinely new immutable heaps; only the synthetic input factory changes."""
    office_b = _raw_offices()[1]
    with monkeypatch.context() as input_context:
        input_context.setattr(source_fixtures, "_raw_offices", lambda: [office_b, dict(office_b)])
        await _canonical_source_inputs(context, input_context, office_b)
        source_pin, _ = await source_fixtures._archive_source(context.fixture, coordinates, owner, role)
    fixture = context.fixture
    await fixture.connection.execute(f'GRANT "{role}" TO "{fixture.roles["reader"]}" WITH INHERIT TRUE, SET FALSE')
    actor = _actor()
    bindings = [
        _aca_binding(coordinates, network["record_id"], index + 1) for index, network in enumerate(context.networks)
    ]
    written = await _write_aca_bindings(fixture.connection, fixture.control_schema, actor, bindings)
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *written["records"]),
        actor,
    )
    return RegistrySourceMembershipRecipe(source_pin, coordinates), approved["approved_revision"]


def _next_coordinates(source_pin):
    """Allocate a new exact retained source namespace and edition."""
    dataset = uuid4()
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates

    declaration = source_pin.prepared.manifest.source_metadata["network_bindings"]
    coordinates = RegistryNetworkSourceCoordinates(
        **{key: coordinate for key, coordinate in declaration.items() if key != "source_key_kind"}
    )
    coordinates = replace(
        coordinates,
        dataset_schema=archive.reference_family_stage_schema(dataset),
        dataset_id=str(dataset),
        edition_id="edition-two",
    )
    return coordinates


async def test_native_protected_post_retained_source_does_not_borrow_new_source_heaps(transport_db, monkeypatch):
    """Publish S1 OfficeB and still read OfficeA only from pinned physical S0."""
    context = transport_db
    fixture = context.fixture
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        original = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    owner, role = "http_aca_owner_" + uuid4().hex, "http_aca_reader_" + uuid4().hex
    coordinates = _next_coordinates(context.source_pin)
    address_schema = "http_aca_address_" + uuid4().hex
    # Cleanup is registered before preparation can create its schema/roles.
    try:
        recipe, revision = await _admit_next_source(context, monkeypatch, coordinates, owner, role)
        next_base = await _next_physical_addresses(context, owner, role, address_schema)
        next_fixture = SimpleNamespace(**(vars(fixture) | {"base": next_base}))
        current, effective, retained_members = await _compose_intent_candidate(
            (next_fixture, fixture.extra_targets, fixture.extra_requests, context.networks[0]["record_id"]),
            revision,
            original,
            original.generation_id,
            (recipe,),
        )
        assert len(effective) == len(retained_members) == 1
        assert recipe.source_pin.prepared.ownership.schema_name != context.source_pin.prepared.ownership.schema_name
        excluded_ids = [context.networks[1]["record_id"]]
        document = v2_envelope({"npi": "1000000491"}, excluded_network_ids=excluded_ids, policy_revision="9")
        live, transactions = await _post(context, document)
        assert live.status == 200 and transactions == 1
        assert live.json["generation"] == str(current.generation_id)
        assert live.json["npi_data"][0]["npi_info"]["addresses"] == [_raw_offices()[1], _raw_offices()[1]]
        retained_document = v2_envelope(
            {"npi": "1000000491", "network_generation": str(original.generation_id)},
            excluded_network_ids=excluded_ids,
            policy_revision="9",
        )
        old, transactions = await _post(context, retained_document)
        assert old.status == 200 and transactions == 1
        assert old.json["npi_data"][0]["npi_info"]["addresses"] == _raw_offices()[:1]
        _success_headers(live, document)
        _success_headers(old, retained_document)
        assert live.headers["X-Network-Access-Scope"] == old.headers["X-Network-Access-Scope"]
    finally:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{address_schema}" CASCADE')
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{coordinates.dataset_schema}" CASCADE')
        for role_name in (role, owner):
            if await fixture.connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role_name):
                await fixture.connection.execute(f'DROP OWNED BY "{role_name}"')
                await fixture.connection.execute(f'DROP ROLE "{role_name}"')
