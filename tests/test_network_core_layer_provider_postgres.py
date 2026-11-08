# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect the real native reader HTTP routes to the compiled public router probe."""

import asyncio
import json
import os
import signal
import socket
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import aiohttp
import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import network_provider_only_read as provider_only
from api.endpoint import network_providers, network_serving
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import encode_source_inventory_reference, verify_source_recipe_metadata
from process.registry_source_inventory_store import source_inventory_entry
from tests.test_network_provider_routes_postgres import _durable_state
from tests.test_registry_generic_fhir_reuse_postgres import (
    _add_mapping,
    _assert_independent_owners,
    _compose,
    _publish,
    _read,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_capture_db as generic_capture_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_composition_db as generic_composition_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio
_TOKEN = "synthetic-network-token"
_NATIVE_TEST = (
    "routes::providers::tests::registry_network_reads::"
    "native_registry_office_and_provider_pages_preserve_core_pins_and_policy"
)


async def _layer_probe(binary, layer_root, base_url, network_id, generations, reader_errors):
    """Run one compiled case; its own process group bounds every child, including Redis."""
    process = await asyncio.create_subprocess_exec(
        str(binary),
        _NATIVE_TEST,
        "--ignored",
        "--exact",
        "--nocapture",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
        cwd=layer_root,
        env=os.environ
        | {
            "NETWORK_PROVIDER_TEST_CORE_URL": base_url,
            "NETWORK_PROVIDER_TEST_NETWORK_ID": str(network_id),
            "NETWORK_PROVIDER_TEST_GENERATIONS": ",".join(map(str, generations)),
            "HEALTHPORTA_REQUIRE_REDIS_TESTS": "1",
        },
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=90)
        probe_output = output.decode("utf-8", errors="replace")
        print(probe_output, end="")
        assert process.returncode == 0, (probe_output, reader_errors)
        assert "1 passed; 0 failed" in probe_output, probe_output
    finally:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        if process.returncode is None:
            await asyncio.wait_for(process.wait(), timeout=5)


def _observe_provider_queries(monkeypatch):
    """Forward real provider query preparation, including its native custody and approved-scope checks."""
    errors, schemas = [], []
    native_query = provider_only._page_query

    async def trace_query(*arguments, **keywords):
        control_schema = arguments[2] if len(arguments) > 2 else keywords.get("control_schema")
        schemas.append(provider_only._control_namespace(control_schema)[1:-1])
        try:
            query, parameters = await native_query(*arguments, **keywords)
            assert "network_provider_only_evidence" in query and "network_source_inventory" in query
            return query, parameters
        except Exception as error:
            errors.append((keywords.get("control_schema"), repr(error), repr(error.__cause__)))
            raise

    monkeypatch.setattr(provider_only, "_page_query", trace_query)
    return errors, schemas


@pytest.mark.parametrize("control_schema", [None, "synthetic_control"])
async def test_provider_query_observer_preserves_native_results(monkeypatch, control_schema):
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "synthetic_control")
    arguments = [1]
    query = "SELECT * FROM network_source_inventory JOIN network_provider_only_evidence ON true"
    native_query = AsyncMock(return_value=(query, arguments))
    monkeypatch.setattr(provider_only, "_page_query", native_query)
    errors, schemas = _observe_provider_queries(monkeypatch)
    connection, manifest = object(), object()
    observed_query, observed_arguments = await provider_only._page_query(connection, manifest, control_schema)
    assert observed_query == query and observed_arguments is arguments
    native_query.assert_awaited_once_with(connection, manifest, control_schema)
    assert errors == [] and schemas == ["synthetic_control"]


def _observe_manifest_resolution(monkeypatch):
    """Inspect the real request's native transaction while forwarding every resolver call."""
    errors, settings = [], []
    native_resolver = network_serving.resolve_network_serving_manifest

    async def trace_resolution(*arguments, **keywords):
        settings.append(
            tuple(
                await arguments[0].fetchrow(
                    "SELECT current_user,session_user,current_setting('transaction_isolation'),"
                    "current_setting('transaction_read_only')"
                )
            )
        )
        try:
            return await native_resolver(*arguments, **keywords)
        except Exception as error:
            errors.append((repr(error), repr(error.__cause__)))
            raise

    monkeypatch.setattr(network_serving, "resolve_network_serving_manifest", trace_resolution)
    return errors, settings


def _native_layer_inputs():
    supplied = os.getenv("NETWORK_PROVIDER_TEST_LAYER_BINARY")
    if not supplied:
        pytest.skip("NETWORK_PROVIDER_TEST_LAYER_BINARY must select the compiled native router probe")
    binary = Path(supplied).resolve(strict=True)
    assert binary.is_file() and os.access(binary, os.X_OK)
    layer_root = Path(os.environ["NETWORK_PROVIDER_TEST_LAYER_ROOT"]).resolve(strict=True)
    assert (layer_root / "Cargo.toml").is_file() and (layer_root / "config/plans.toml").is_file()
    return binary, layer_root


async def _published_generations(context):
    """Keep genuine capture/custody and reuse unchanged physical facts through constant inventory metadata."""
    fixture = context.fixture
    original = await _publish(context, await _compose(context, context.revision), first=True)
    original_descriptor, _derivation = await _inventory_metadata(fixture, original, office_revision=5)
    assert original_descriptor["fact_candidate_id"] is None
    fact_kinds = await fixture.connection.fetch(
        "SELECT fact_json->>'kind' AS kind,count(*) AS fact_rows,"
        "bool_and(fact_json->'fact_version'='2'::jsonb) AS witness_v2 "
        f'FROM "{original.schema_name}".network_source_contribution GROUP BY 1'
    )
    assert {fact_kind["kind"]: fact_kind["fact_rows"] for fact_kind in fact_kinds} == {"resource": 5, "edge": 9}
    assert all(fact_kind["witness_v2"] for fact_kind in fact_kinds)
    before = await _read(context, original)
    await _assert_independent_owners(context)
    revision = await _add_mapping(context, "network-unapproved")
    rebound = await _publish(context, await _compose(context, revision, original))
    assert rebound.generation_id > original.generation_id
    assert await _read(context, original) == before
    descriptor, derivation = await _inventory_metadata(fixture, rebound, office_revision=5)
    assert descriptor["fact_candidate_id"] == original.candidate_id
    assert descriptor["fact_sha256"] == original_descriptor["fact_sha256"]
    assert descriptor["fact_rows"] == original_descriptor["fact_rows"] == 14
    assert descriptor["membership_rows"] == 3
    candidate_by_field = dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(rebound.candidate_id),
        )
    )
    summary = verify_initial_source_office_receipt(candidate_by_field)
    assert (summary["revision"], summary["office_rows"]) == (5, 1)
    assert rebound.source_generations["retained_network_source"] == original.manifest_sha256
    assert summary["origin_manifest_sha256"] is None
    assert summary["address_generation_sha256"] == context.address.generation_id
    assert derivation["derivation_version"] == 2
    assert derivation["origin_manifest_sha256"] is None
    assert derivation["address_source"] == asdict(context.address)
    assert derivation["fact_artifact"]["fact_candidate_id"] == original.candidate_id
    assert rebound.compact_layout is not None and original.compact_layout is not None
    assert rebound.compact_layout.base.table_oid == original.compact_layout.base.table_oid
    assert derivation["address_identity"]["table_oid"] == original.compact_layout.base.table_oid
    return original, rebound, before


async def _assert_inventory_migration(fixture):
    definition = await fixture.connection.fetchval(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=to_regclass($1) "
        "AND conname='network_candidate_source_recipes' AND contype='c'",
        f'"{fixture.control_schema}".network_membership_candidate',
    )
    assert definition is not None and all(
        field in definition
        for field in (
            "registry_source_inventory",
            "network_source_inventory",
            "encoded_bytes",
            "recipe_bytes",
            "67108864",
        )
    )


async def _inventory_metadata(fixture, manifest, *, office_revision=4):
    """Assert bounded registered metadata; the already resolved manifest retains native custody authority."""
    candidate_by_field = dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(manifest.candidate_id),
        )
    )
    metadata = verify_source_recipe_metadata(candidate_by_field)
    assert metadata.inventory is not None
    root = metadata.inventory.root
    assert (root.entry_count, root.contribution_count, root.derivation_count) == (1, 1, 1)
    assert len(encode_source_inventory_reference(metadata.inventory).encode()) <= 4096
    generations = candidate_by_field["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    assert generations["registry_source_inventory"] == root.entries_sha256
    assert not any(key.startswith("initial_source_recipe_") for key in generations)
    inventory_records = await fixture.connection.fetch(
        f'SELECT * FROM "{manifest.schema_name}".network_source_inventory ORDER BY ordinal'
    )
    entries = tuple(source_inventory_entry(inventory_record) for inventory_record in inventory_records)
    assert source_inventory_root(entries) == root and len(entries) == 1
    contributions = verify_registry_source_contributions(candidate_by_field)
    office = verify_initial_source_office_receipt(candidate_by_field)
    assert office_revision in (4, 5)
    assert contributions["revision"] == (4 if office_revision == 5 else 3) and "sources" not in contributions
    assert office["revision"] == office_revision and "derivations" not in office
    if office_revision == 5:
        assert json.loads(entries[0].contribution_json)["descriptor_version"] == 2
        assert json.loads(entries[0].office_derivation_json)["derivation_version"] == 2
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", manifest.schema_name + ".entity_address_unified")
        is None
    )
    return json.loads(entries[0].contribution_json), json.loads(entries[0].office_derivation_json)


async def _http_reader_url(fixture, supplied):
    """Keep an existing native Reader's credentials and privileges unchanged."""
    reader = fixture.roles["reader"]
    if supplied is not None:
        assert supplied.username == reader
        assert supplied.database == fixture.engine.url.database
        assert (supplied.host, supplied.port, supplied.query) == (
            fixture.engine.url.host,
            fixture.engine.url.port,
            fixture.engine.url.query,
        )
        return supplied
    password = uuid4().hex
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
    )
    await fixture.connection.execute(f"ALTER ROLE \"{reader}\" LOGIN PASSWORD '{password}'")
    return fixture.engine.url.set(username=reader, password=password)


@asynccontextmanager
async def _reader_http(fixture, *, reader_url=None):
    """Expose actual blueprints through a distinct native Reader engine."""
    reader = fixture.roles["reader"]
    engine = create_async_engine(await _http_reader_url(fixture, reader_url))
    statements, requests = [], []
    event.listen(engine.sync_engine, "before_cursor_execute", lambda *arguments: statements.append(arguments[2]))
    sessions = async_sessionmaker(engine)
    app = Sanic("native_network_provider_transport_" + uuid4().hex)
    app.blueprint(Blueprint.group([network_providers.blueprint, network_serving.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        request.ctx.sa_session = sessions()
        requests.append((request.path, request.headers.get("X-Network-Access-Scope")))

    @app.middleware("response")
    async def close_session(request, _response):
        await request.ctx.sa_session.close()

    server = None
    listener = socket.socket()
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            assert tuple((await connection.execute(text("SELECT current_user,session_user"))).one()) == (reader, reader)
            assert not await connection.scalar(
                text("SELECT pg_has_role(current_user,CAST(:owner AS name),'SET')"),
                {"owner": fixture.writer_roles["owner_role"]},
            )
            driver = (await connection.get_raw_connection()).driver_connection
            await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema)
        listener.bind(("127.0.0.1", 0))
        listener.setblocking(False)
        server = await app.create_server(sock=listener, access_log=False)
        await server.startup()
        await server.start_serving()
        base_url = f"http://127.0.0.1:{server.server.sockets[0].getsockname()[1]}"
        yield base_url, statements, requests
    finally:
        if server is not None:
            await server.close()
            await server.wait_closed()
            assert not server.server.is_serving()
        listener.close()
        await engine.dispose()


async def _assert_core_geo_pages(client, base_url, network_id, generations):
    """Read the retained and current offices through native HTTP geo selection."""
    header_by_name = {"Authorization": "Bearer " + _TOKEN}
    endpoint = base_url + "/api/v1/registry/serving/providers"
    latest = None
    for generation in generations:
        parameter_by_name = {"network_ids": str(network_id), "network_generation": str(generation)}
        async with client.get(endpoint, params=parameter_by_name, headers=header_by_name) as response:
            assert response.status == 200
            office = await response.json()
            assert response.headers["X-Network-Generation"] == str(generation)
        assert office["generation_id"] == generation and office["total_count"] == 1
        assert len(office["providers"]) == 1 and len(office["providers"][0]["locations"]) == 1
        for latitude, longitude, expected_count in ((41, -87, 1), (0, 0, 0)):
            geo_by_name = {"lat": latitude, "long": longitude, "radius_miles": 1}
            async with client.get(endpoint, params=parameter_by_name | geo_by_name, headers=header_by_name) as response:
                assert response.status == 200
                nearby = await response.json()
            assert nearby["generation_id"] == generation and nearby["total_count"] == expected_count
            assert nearby["providers"] == (office["providers"] if expected_count else [])
        async with client.get(
            endpoint, params=parameter_by_name | {"membership_level": "provider"}, headers=header_by_name
        ) as response:
            assert response.status == 200
            provider_only = await response.json()
        assert provider_only["generation_id"] == generation and provider_only["total_count"] == 1
        assert all("locations" not in provider for provider in provider_only["providers"])
        async with client.get(
            endpoint,
            params=parameter_by_name | {"membership_level": "provider", "lat": 41, "long": -87, "radius_miles": 1},
            headers=header_by_name,
        ) as response:
            assert response.status == 400
            assert await response.json() == {"error": {"code": "network_provider_request_invalid"}}
        latest = office
    async with client.get(endpoint, params={"network_ids": str(network_id)}, headers=header_by_name) as response:
        assert response.status == 200 and await response.json() == latest


async def _assert_core_layer_reads(fixture, monkeypatch, *, layer_inputs, network_id, generations, reader_url=None):
    """Reuse real Core routes and the existing compiled policy/cache probe."""
    binary, layer_root = layer_inputs
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", _TOKEN)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    resolution_errors, native_settings = _observe_manifest_resolution(monkeypatch)
    reader_errors, query_schemas = _observe_provider_queries(monkeypatch)
    durable_before = await _durable_state(fixture)
    async with _reader_http(fixture, reader_url=reader_url) as (base_url, statements, requests):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as client:
            async with client.get(
                base_url + "/api/v1/registry/serving/manifest", headers={"Authorization": "Bearer " + _TOKEN}
            ) as response:
                assert response.status == 200, resolution_errors
                assert (await response.json())["generation_id"] == generations[-1]
            await _assert_core_geo_pages(client, base_url, network_id, generations)
        await _layer_probe(
            binary,
            layer_root,
            base_url,
            network_id,
            generations,
            reader_errors,
        )
        scoped_requests = [(path, scope) for path, scope in requests if scope is not None]
        assert len(scoped_requests) == 4  # One native fill for each generation/mode; repeats use the real cache.
        assert all(path.endswith("/providers") and len(scope) == 64 for path, scope in scoped_requests)
        reader = fixture.roles["reader"]
        assert native_settings and all(
            settings == (reader, reader, "repeatable read", "on") for settings in native_settings
        )
        assert query_schemas and all(schema == fixture.control_schema for schema in query_schemas)
        assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in statements
        assert await _durable_state(fixture) == durable_before


@pytest.mark.parametrize("generic_capture_db", ["modern"], indirect=True)
async def test_real_core_layer_transport_preserves_two_custodied_generations(generic_composition_db, monkeypatch):
    """Native transport proof; the public router's App policy authority remains synthetic."""
    layer_inputs = _native_layer_inputs()
    context, fixture = generic_composition_db, generic_composition_db.fixture
    await _assert_inventory_migration(fixture)
    original, rebound, before = await _published_generations(context)
    await _assert_core_layer_reads(
        fixture,
        monkeypatch,
        layer_inputs=layer_inputs,
        network_id=context.network_id,
        generations=(original.generation_id, rebound.generation_id),
    )
    assert await _read(context, original) == before
