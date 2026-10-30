# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native provider HTTP reads through approved composition and protected reader roles."""

import asyncio
import json
from contextlib import aclosing, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from sanic import Blueprint, Sanic, response
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import network_provider_read as native_providers
from api.endpoint import network_providers as providers
from api.network_address_scope import current_network_address_scope
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from tests.test_network_custom_address_source_postgres import _seed, custom_db
from tests.test_network_serving_routes_postgres import _reader_engine
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_candidate_composition_postgres import _imported_candidate, _remove_candidates, _roles

pytestmark = pytest.mark.asyncio
_AUTH = {"Authorization": "Bearer synthetic-network-provider-token"}
_PATH = "/api/v1/registry/serving/providers"


async def _composed_candidate(fixture, seed, source_manifest, request_id, copy_targets):
    """Build and publish the actual approved manual office alongside retained source membership."""
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=seed.revision,
        expected_head=source_manifest.generation_id,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(source_manifest.schema_name, "entity_address_unified", source_manifest.manifest_sha256)
        ),
        writer_roles=_roles(fixture),
        source_manifest=source_manifest,
        control_schema=fixture.control_schema,
    )
    copy_targets.append(copy_target)
    await prepare_and_publish_network_candidate(
        fixture.connection, copy_target, addresses, **_roles(fixture), control_schema=fixture.control_schema
    )


@pytest.fixture
async def provider_app(custom_db, monkeypatch):
    """Own exact candidate resources, real ASGI sessions and a SELECT-only native role."""
    fixture = custom_db
    copy_targets = []
    request_id = uuid4()
    engine = None
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-provider-token")
    try:
        source_manifest = await _imported_candidate(fixture, copy_targets)
        source_network_id = await fixture.connection.fetchval(
            f'SELECT network_id FROM "{source_manifest.schema_name}".network_membership'
        )
        seed = await _seed(fixture)
        await _composed_candidate(fixture, seed, source_manifest, request_id, copy_targets)
        await fixture.connection.execute(
            f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
        )
        await fixture.connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
        )
        engine = _reader_engine(fixture.engine, fixture.roles["reader"])
        sessions = async_sessionmaker(engine)
        app = Sanic("network_providers_http_" + uuid4().hex)
        app.blueprint(Blueprint.group([providers.blueprint], version_prefix="/api/v"))

        @app.middleware("request")
        async def open_session(request):
            """Create a lazy session so rejected requests acquire no database connection."""
            request.ctx.sa_session = sessions()

        @app.middleware("response")
        async def close_session(request, _response):
            """Release every request-owned read transaction after success or failure."""
            await request.ctx.sa_session.close()

        yield SimpleNamespace(
            app=app,
            fixture=fixture,
            engine=engine,
            seed=seed,
            source_manifest=source_manifest,
            source_network_id=source_network_id,
            current_schema=copy_targets[-1].schema_name,
        )
    finally:
        if engine is not None:
            await engine.dispose()
        await _remove_candidates(fixture, copy_targets, request_id)


async def _durable_state(fixture):
    """Compare durable control, heads, history and manifests around HTTP reads."""
    return await fixture.connection.fetchrow(
        f'''SELECT (SELECT to_jsonb(control) FROM "{fixture.control_schema}".registry_revision_control control) AS control,
        (SELECT count(*) FROM "{fixture.control_schema}".registry_record_history) AS history,
        (SELECT count(*) FROM "{fixture.control_schema}".registry_approved_record) AS approved,
        (SELECT jsonb_agg(to_jsonb(manifest) ORDER BY generation_id)
          FROM "{fixture.control_schema}".network_serving_manifest manifest) AS manifests,
        (SELECT jsonb_agg(to_jsonb(candidate) ORDER BY candidate_id)
          FROM "{fixture.control_schema}".network_membership_candidate candidate) AS candidates'''
    )


async def test_manual_provider_without_npi_has_only_approved_selected_office(provider_app, monkeypatch):
    context = provider_app
    before = await _durable_state(context.fixture)
    native_read = providers.read_network_provider_page
    observed_settings = []

    async def observe_transaction(connection, scope, **kwargs):
        """Observe the real role, transaction and trusted scope before the actual native read."""
        observed_settings.append(
            await connection.fetchrow(
                "SELECT current_user,current_setting('transaction_isolation') AS isolation,"
                "current_setting('transaction_read_only') AS readonly"
            )
        )
        assert current_network_address_scope() is scope and scope.access_scope_sha256 == "a" * 64
        return await native_read(connection, scope, **kwargs)

    monkeypatch.setattr(providers, "read_network_provider_page", observe_transaction)
    network_id = context.seed.network["record_id"]
    _, page = await context.app.asgi_client.get(
        _PATH + f"?network_ids={network_id}", headers={**_AUTH, "X-Network-Access-Scope": "a" * 64}
    )
    assert page.status == 200 and page.json["total_count"] == 1
    assert (page.json["limit"], page.json["offset"], page.json["has_more"]) == (50, 0, False)
    provider = page.json["providers"][0]
    assert (provider["provider_system"], provider["provider_id"], provider["npi"]) == (
        "manual",
        context.seed.provider["record_id"],
        None,
    )
    assert [office["location_id"] for office in provider["locations"]] == [context.seed.first["record_id"]]
    assert context.seed.second["record_id"] not in page.text
    assert page.headers["Cache-Control"] == "private, no-store"
    assert page.headers["X-Network-Generation"] == str(page.json["generation_id"])
    assert tuple(observed_settings[0]) == (context.fixture.roles["reader"], "repeatable read", "on")
    assert current_network_address_scope() is None
    _, detail = await context.app.asgi_client.get(
        _PATH + f"/manual/{provider['provider_id']}?network_ids={network_id}", headers=_AUTH
    )
    assert detail.status == 200 and detail.json == {"generation_id": page.json["generation_id"], "provider": provider}
    _, empty = await context.app.asgi_client.get(
        _PATH + f"?network_ids={network_id}&limit=1&offset=1", headers={**_AUTH, "X-Network-Access-Scope": "a" * 64}
    )
    assert empty.status == 200 and empty.json["providers"] == [] and empty.json["total_count"] == 1
    assert await _durable_state(context.fixture) == before
    async with context.engine.connect() as connection:
        assert await connection.scalar(text("SELECT current_setting('transaction_read_only')")) == "off"


@asynccontextmanager
async def _provider_child_relations(context, network_id):
    fixture = context.fixture
    provider_id, location_id, location_key = str(uuid4()), str(uuid4()), uuid4().hex * 2
    identity_by_field = dict(provider_system="manual", provider_id=provider_id, location_id=location_id)
    changes_by_table = {
        "network_membership": dict(**identity_by_field, network_id=network_id, evidence_id="unsealed-child"),
        "provider_location_binding": dict(
            **identity_by_field, location_key=location_key, entity_type="manual", entity_id=provider_id
        ),
        "entity_address_unified": dict(
            location_key=location_key,
            entity_type="manual",
            entity_id=provider_id,
            entity_name="Unsealed child provider",
            npi=None,
            canonical_network_ids=[network_id],
        ),
    }
    child_relations = []
    try:
        for table, changes in changes_by_table.items():
            parent = f'"{context.current_schema}"."{table}"'
            child = f'"{fixture.source_schema}"."provider_child_{table}"'
            await fixture.connection.execute(f"CREATE TABLE {child} (LIKE {parent} INCLUDING CONSTRAINTS)")
            child_relations.append((child, parent))
            await fixture.connection.execute(
                f"INSERT INTO {child} SELECT (jsonb_populate_record(NULL::{parent},to_jsonb(row)||$1::jsonb)).* "
                f"FROM ONLY {parent} row LIMIT 1",
                json.dumps(changes),
            )
        yield child_relations, provider_id
    finally:
        for child, _ in reversed(child_relations):
            await fixture.connection.execute(f"DROP TABLE {child}")


async def _assert_inherited_provider_leak(connection, manifest, network_id, provider_id, original_guard):
    addresses, address_arguments = native_providers._provider_addresses(manifest, (network_id,))
    inherited_query = (
        native_providers._PROVIDERS_SQL.format(namespace=f'"{manifest.schema_name}"', addresses=addresses)
        .replace("FROM ONLY ", "FROM ")
        .replace("JOIN ONLY ", "JOIN ")
    )
    inherited = await connection.fetchrow(
        inherited_query,
        "{" + str(network_id) + "}",
        None,
        None,
        50,
        0,
        native_providers.MAX_LOCATIONS,
        native_providers.MAX_RESPONSE_BYTES,
        manifest.generation_id,
        None,
        None,
        None,
        None,
        *address_arguments,
    )
    await original_guard(
        connection,
        native_providers._provider_read_relations(manifest),
        expected_backend_pid=connection.get_server_pid(),
    )
    assert provider_id in json.dumps(native_providers._result(inherited))


async def test_raw_provider_read_excludes_children_attached_after_metadata(provider_app, monkeypatch):
    context = provider_app
    fixture = context.fixture
    network_id = context.seed.network["record_id"]
    _, baseline = await context.app.asgi_client.get(_PATH + f"?network_ids={network_id}", headers=_AUTH)
    assert baseline.status == 200 and baseline.json["total_count"] == 1
    async with _provider_child_relations(context, network_id) as (child_relations, provider_id):
        original_resolver = native_providers.resolve_network_serving_manifest
        original_guard = native_providers.verify_consumed_network_relations
        attached, guarded = [], []

        async def attach_after_metadata(connection, **arguments):
            manifest = await original_resolver(connection, **arguments)
            assert manifest.compact_layout is None
            for child, parent in child_relations:
                await fixture.connection.execute(f"ALTER TABLE {child} INHERIT {parent}")
                attached.append(child)
            await _assert_inherited_provider_leak(connection, manifest, network_id, provider_id, original_guard)
            return manifest

        async def guard(connection, relations, **arguments):
            await original_guard(connection, relations, **arguments)
            guarded.append((connection.get_server_pid(), len(relations)))

        monkeypatch.setattr(native_providers, "resolve_network_serving_manifest", attach_after_metadata)
        monkeypatch.setattr(native_providers, "verify_consumed_network_relations", guard)
        _, selected = await context.app.asgi_client.get(_PATH + f"?network_ids={network_id}", headers=_AUTH)
        assert attached == [child for child, _ in child_relations]
        assert selected.status == 200 and selected.json == baseline.json
        assert provider_id not in selected.text
        assert len(guarded) == 1 and guarded[0][1] == 3
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{context.current_schema}".network_membership WHERE network_id=$1', network_id
            )
            == 2
        )


async def test_current_and_retained_generations_keep_exact_identity_namespaces(provider_app):
    context = provider_app
    both_networks = f"{context.source_network_id},{context.seed.network['record_id']}"
    _, current = await context.app.asgi_client.get(_PATH + f"?network_ids={both_networks}", headers=_AUTH)
    assert current.status == 200
    assert {(provider["provider_system"], provider["provider_id"]) for provider in current.json["providers"]} == {
        ("npi", "1000000004"),
        ("manual", context.seed.provider["record_id"]),
    }
    retained_query = (
        f"?network_ids={context.source_network_id}&network_generation={context.source_manifest.generation_id}"
    )
    _, retained = await context.app.asgi_client.get(_PATH + "/npi/1000000004" + retained_query, headers=_AUTH)
    assert retained.status == 200 and retained.json["generation_id"] == context.source_manifest.generation_id
    assert len(retained.json["provider"]["locations"]) == 1
    assert retained.headers["X-Network-Generation"] == str(context.source_manifest.generation_id)
    for identity in ("npi/1000000004", "provider_directory/1000000004", f"manual/{uuid4()}"):
        _, missing = await context.app.asgi_client.get(
            _PATH + "/" + identity + f"?network_ids={context.seed.network['record_id']}", headers=_AUTH
        )
        assert missing.status == 404 and missing.json == {"error": {"code": "network_provider_not_found"}}
        assert missing.headers["Cache-Control"] == "private, no-store"
        assert missing.headers["X-Network-Generation"] == str(current.json["generation_id"])


async def test_auth_and_closed_request_validation_precede_database_access(provider_app):
    context = provider_app
    statements = []
    event.listen(
        context.engine.sync_engine, "before_cursor_execute", lambda *arguments: statements.append(arguments[2])
    )
    for headers in ({}, {"Authorization": "Bearer incorrect-token"}):
        _, denied = await context.app.asgi_client.get(_PATH + "?network_ids=0", headers=headers)
        assert denied.status == 403 and denied.headers["Cache-Control"] == "private, no-store"
    for query in (
        "",
        "network_ids=",
        "network_ids=0",
        "network_ids=01",
        "network_ids=-1",
        "network_ids=true",
        "network_ids=1.0",
        "network_ids=2147483648",
        "network_ids=1,1",
        "network_ids=1&network_ids=1",
        "network_ids=1&extra=1",
        "network_generation=1",
        "network_ids=1&network_generation=",
        "network_ids=1&network_generation=01",
        "network_ids=1&network_generation=9223372036854775808",
        "network_ids=1&network_generation=1&network_generation=1",
        "network_ids=1&limit=0",
        "network_ids=1&limit=101",
        "network_ids=1&limit=01",
        "network_ids=1&offset=-1",
        "network_ids=1&offset=1000001",
        "network_ids=1&offset=0&offset=0",
        "network_ids",
        "network_ids=1&checksum=42",
        "network_ids=" + "1" * 2050,
    ):
        _, invalid = await context.app.asgi_client.get(_PATH + "?" + query, headers=_AUTH)
        assert invalid.status == 400 and invalid.json == {"error": {"code": "network_provider_request_invalid"}}
    for identity in ("unknown/value", "npi/1111111111", "manual/not-a-uuid"):
        _, invalid = await context.app.asgi_client.get(_PATH + "/" + identity + "?network_ids=1", headers=_AUTH)
        assert invalid.status == 400
    for suffix in ("&limit=1", "&offset=0"):
        _, invalid = await context.app.asgi_client.get(_PATH + "/npi/1000000004?network_ids=1" + suffix, headers=_AUTH)
        assert invalid.status == 400
    _, invalid_header = await context.app.asgi_client.get(
        _PATH + "?network_ids=1", headers={**_AUTH, "X-Network-Access-Scope": "A" * 64}
    )
    assert invalid_header.status == 400
    _, body = await context.app.asgi_client.request("GET", _PATH + "?network_ids=1", headers=_AUTH, content=b"{}")
    assert body.status == 400 and statements == []


@pytest.mark.parametrize("damage", ["retired", "digest", "oid", "permission", "identity", "missing_generation"])
async def test_unavailable_results_are_sanitized_and_do_not_fall_back(provider_app, damage):
    context = provider_app
    fixture = context.fixture
    namespace = f'"{fixture.control_schema}"'
    query = f"?network_ids={context.seed.network['record_id']}"
    if damage in ("retired", "digest"):
        assignment = "eligible=false" if damage == "retired" else "manifest_sha256='" + "0" * 64 + "'"
        await fixture.connection.execute(
            f"UPDATE {namespace}.network_serving_manifest SET {assignment} WHERE generation_id="
            f"(SELECT generation_id FROM {namespace}.network_serving_control)"
        )
    elif damage == "oid":
        await fixture.connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET validation_json="
            "jsonb_set(validation_json,'{writer_closure,relation_oids,entity_address_unified}','1'::jsonb) "
            "WHERE schema_name=$1",
            context.current_schema,
        )
    elif damage == "permission":
        await fixture.connection.execute(
            f'REVOKE SELECT ON "{context.current_schema}".network_membership FROM "{fixture.roles["reader"]}"'
        )
    elif damage == "identity":
        await fixture.connection.execute(
            f"UPDATE \"{context.current_schema}\".entity_address_unified SET entity_id=$1 WHERE entity_type='manual'",
            str(uuid4()),
        )
    else:
        query += "&network_generation=9223372036854775807"
    before = await _durable_state(fixture)
    _, unavailable = await context.app.asgi_client.get(_PATH + query, headers=_AUTH)
    assert unavailable.status == 503 and unavailable.json == {"error": {"code": "network_provider_unavailable"}}
    assert unavailable.headers["Cache-Control"] == "private, no-store"
    assert "X-Network-Generation" not in unavailable.headers
    assert context.current_schema not in unavailable.text and fixture.roles["reader"] not in unavailable.text
    assert await _durable_state(fixture) == before


async def test_native_catalog_lock_timeout_is_bounded_and_releases_transaction(provider_app):
    context = provider_app
    async with context.fixture.engine.connect() as connection, connection.begin():
        await connection.execute(
            text(f'LOCK TABLE "{context.fixture.control_schema}".network_serving_manifest IN ACCESS EXCLUSIVE MODE')
        )
        _, unavailable = await asyncio.wait_for(
            context.app.asgi_client.get(_PATH + f"?network_ids={context.seed.network['record_id']}", headers=_AUTH),
            timeout=3,
        )
    assert unavailable.status == 503
    _, recovered = await context.app.asgi_client.get(
        _PATH + f"?network_ids={context.seed.network['record_id']}", headers=_AUTH
    )
    assert recovered.status == 200


@pytest.mark.parametrize("damage", ["orphan_binding", "unbound_address", "conflicting_office_binding"])
async def test_integrity_outside_the_requested_page_still_refuses(provider_app, damage):
    context = provider_app
    networks = f"{context.source_network_id},{context.seed.network['record_id']}"
    path = _PATH + f"?network_ids={networks}&limit=1"
    _, original = await context.app.asgi_client.get(path, headers=_AUTH)
    assert original.status == 200
    assert original.json["providers"][0]["provider_system"] == "manual"
    namespace = f'"{context.current_schema}"'
    new_key = uuid4().hex + uuid4().hex
    if damage != "conflicting_office_binding":
        await context.fixture.connection.execute(
            f"""INSERT INTO {namespace}.entity_address_unified
            SELECT (jsonb_populate_record(NULL::{namespace}.entity_address_unified,
              to_jsonb(address)||jsonb_build_object('location_key',$1::text))).*
            FROM {namespace}.entity_address_unified address WHERE entity_type='npi'""",
            new_key,
        )
    if damage != "unbound_address":
        await context.fixture.connection.execute(
            f"""INSERT INTO {namespace}.provider_location_binding
            SELECT provider_system,CASE WHEN $2::boolean THEN '1000000491' ELSE provider_id END,$1::uuid,
              CASE WHEN $2::boolean THEN location_key ELSE $3::text END,entity_type,entity_id
            FROM {namespace}.provider_location_binding WHERE provider_system='npi'""",
            uuid4(),
            damage == "conflicting_office_binding",
            new_key,
        )
    _, unavailable = await context.app.asgi_client.get(path, headers=_AUTH)
    assert unavailable.status == 503
    assert unavailable.json == {"error": {"code": "network_provider_unavailable"}}
    assert unavailable.headers["Cache-Control"] == "private, no-store"
    assert "X-Network-Generation" not in unavailable.headers


async def test_coherent_source_office_alias_preserves_the_provider_page(provider_app):
    context = provider_app
    path = _PATH + f"?network_ids={context.source_network_id}"
    _, original = await context.app.asgi_client.get(path, headers=_AUTH)
    assert original.status == 200 and original.json["total_count"] == 1
    namespace = f'"{context.current_schema}"'
    assert (
        await context.fixture.connection.execute(
            f"""INSERT INTO {namespace}.provider_location_binding
        SELECT provider_system,provider_id,$1::uuid,location_key,entity_type,entity_id
        FROM {namespace}.provider_location_binding WHERE provider_system='npi'""",
            uuid4(),
        )
        == "INSERT 0 1"
    )
    _, aliased = await context.app.asgi_client.get(path, headers=_AUTH)
    assert aliased.status == 200 and aliased.json == original.json


@pytest.fixture
async def inflight_provider_http(provider_app, monkeypatch):
    """Own a real loopback listener without changing the protected request middleware."""
    import socket

    import httpx

    monkeypatch.setattr(Sanic, "test_mode", True)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    listener.setblocking(False)
    address = listener.getsockname()
    server = None
    try:
        server = await provider_app.app.create_server(
            sock=listener, access_log=False, asyncio_server_kwargs={"start_serving": False}
        )
        await server.startup()
        await server.before_start()
        await server.start_serving()
        await server.after_start()
        async with httpx.AsyncClient(base_url=f"http://{address[0]}:{address[1]}", timeout=35) as client:
            yield client
    finally:
        if server is not None:
            await asyncio.wait_for(server.before_stop(), timeout=5)
            await asyncio.wait_for(server.close(), timeout=5)
            await asyncio.wait_for(server.after_stop(), timeout=5)
            assert not server.is_serving()
        listener.close()


async def test_loopback_startup_preserves_later_asgi_middleware(monkeypatch):
    """A real listener must not poison Sanic's original methods for the next ASGI app."""
    monkeypatch.setattr(Sanic, "test_mode", False)
    applications = [Sanic("provider_startup_" + uuid4().hex) for _ in range(2)]
    for application in applications:
        application.get("/startup")(lambda _request: response.json({"ready": True}))

    @applications[1].middleware("response")
    async def mark_response(_request, reply):
        reply.headers["X-Startup-Order"] = "preserved"

    try:
        fixture = inflight_provider_http.__wrapped__(SimpleNamespace(app=applications[0]), monkeypatch)
        async with aclosing(fixture):
            client = await anext(fixture)
            reply = await client.get("/startup")
            assert reply.status_code == 200 and reply.json() == {"ready": True}
        for _ in range(2):
            _, reply = await applications[1].asgi_client.get("/startup")
            assert reply.status == 200 and reply.json == {"ready": True}
            assert reply.headers["X-Startup-Order"] == "preserved"
    finally:
        for application in applications:
            Sanic.unregister_app(application)


async def _observe_provider_pin(connection, manifest, context):
    """Read the real request role and exact INTEGER office arrays before holding it."""
    settings = await connection.fetchrow(
        "SELECT current_user,pg_backend_pid() AS pid,"
        "current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS readonly"
    )
    assert tuple(settings[name] for name in ("current_user", "isolation", "readonly")) == (
        context.fixture.roles["reader"],
        "repeatable read",
        "on",
    )
    assert settings["pid"] != await context.fixture.connection.fetchval("SELECT pg_backend_pid()")
    office_rows = await connection.fetch(
        "SELECT binding.provider_system,binding.provider_id,binding.location_id::text AS location_id,"
        "address.canonical_network_ids,pg_typeof(address.canonical_network_ids)::text AS array_type "
        f'FROM "{manifest.schema_name}".provider_location_binding binding '
        f'JOIN "{manifest.schema_name}".entity_address_unified address '
        "USING(location_key,entity_type,entity_id) "
        "WHERE address.canonical_network_ids && $1::integer[] ORDER BY provider_system,provider_id,location_id",
        [context.source_network_id, context.seed.network["record_id"]],
    )
    assert all(office["array_type"] == "integer[]" for office in office_rows)
    assert all(
        type(value) is int and 1 <= value <= 2147483647
        for office in office_rows
        for value in office["canonical_network_ids"]
    )
    return office_rows


def _held_manifest_resolver(native_manifest, context, pinned, release, snapshots):
    """Forward real resolution, then use Events to control only the in-flight reader."""

    async def resolve(connection, **kwargs):
        manifest = await native_manifest(connection, **kwargs)
        office_rows = await _observe_provider_pin(connection, manifest, context)
        snapshots[manifest.generation_id] = (manifest, office_rows)
        if not pinned.is_set():
            pinned.set()
            await asyncio.wait_for(release.wait(), timeout=30)
            assert await _observe_provider_pin(connection, manifest, context) == office_rows
        return manifest

    return resolve


async def _publish_provider_replacement(context, old_manifest, request_id, copy_targets):
    """Approve a different office and run the actual composed COPY/publication pipeline."""
    from dataclasses import replace

    from tests.test_network_custom_address_source_postgres import _draft
    from tests.test_network_membership_draft_store_postgres import _member
    from tests.test_registry_approval_store_postgres import _approve, _command

    fixture = context.fixture
    correction = replace(
        context.seed.membership_command,
        operation="correct",
        expected_revision=1,
        fields={
            "memberships_json": [
                _member(
                    context.seed.network["record_id"],
                    context.seed.provider["record_id"],
                    context.seed.second["record_id"],
                )
            ]
        },
        idempotency_key=uuid4().hex,
    )
    corrected = await _draft(fixture, correction, context.seed.actor)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, corrected),
        context.seed.actor,
    )
    replacement = SimpleNamespace(**{**vars(context.seed), "revision": approval["approved_revision"]})
    await _composed_candidate(fixture, replacement, old_manifest, request_id, copy_targets)
    published = await fixture.connection.fetchrow(
        f'SELECT manifest.generation_id,candidate.state,candidate.schema_name FROM "{fixture.control_schema}".network_serving_control control '
        f'JOIN "{fixture.control_schema}".network_serving_manifest manifest USING(generation_id) '
        f'JOIN "{fixture.control_schema}".network_membership_candidate candidate USING(candidate_id)'
    )
    assert published["state"] == "published" and published["generation_id"] != old_manifest.generation_id
    assert published["schema_name"] == copy_targets[-1].schema_name
    assert not fixture.connection.is_in_transaction()
    return approval["approved_revision"], published


def _assert_provider_cutover(context, baseline, held_response, fresh_response, published, snapshots, revision):
    """Compare complete HTTP identities and physical office arrays on both pinned generations."""
    old_document, fresh_document = baseline.json(), fresh_response.json()
    assert held_response.status_code == 200 and held_response.json() == old_document
    assert held_response.headers["X-Network-Generation"] == str(old_document["generation_id"])
    assert fresh_response.status_code == 200 and fresh_document["generation_id"] == published["generation_id"]
    assert fresh_response.headers["X-Network-Generation"] == str(published["generation_id"])
    assert held_response.headers["Cache-Control"] == fresh_response.headers["Cache-Control"] == "private, no-store"
    old_by_system = {provider["provider_system"]: provider for provider in old_document["providers"]}
    fresh_by_system = {provider["provider_system"]: provider for provider in fresh_document["providers"]}
    assert old_document["total_count"] == fresh_document["total_count"] == 2
    assert len(old_document["providers"]) == len(fresh_document["providers"]) == 2
    assert old_by_system["manual"]["provider_id"] == context.seed.provider["record_id"]
    assert fresh_by_system["manual"] == {**old_by_system["manual"], "locations": fresh_by_system["manual"]["locations"]}
    assert fresh_by_system["npi"] == old_by_system["npi"]
    assert [office["location_id"] for office in old_by_system["manual"]["locations"]] == [
        context.seed.first["record_id"]
    ]
    assert [office["location_id"] for office in fresh_by_system["manual"]["locations"]] == [
        context.seed.second["record_id"]
    ]
    assert snapshots[published["generation_id"]][0].approved_custom_revision == revision
    for generation, manual_office in (
        (old_document["generation_id"], context.seed.first["record_id"]),
        (published["generation_id"], context.seed.second["record_id"]),
    ):
        assert {
            (
                office["provider_system"],
                office["provider_id"],
                office["location_id"],
                tuple(office["canonical_network_ids"]),
            )
            for office in snapshots[generation][1]
        } == {
            ("manual", context.seed.provider["record_id"], manual_office, (context.seed.network["record_id"],)),
            ("npi", "1000000004", old_by_system["npi"]["locations"][0]["location_id"], (context.source_network_id,)),
        }


async def test_inflight_http_reader_keeps_old_offices_through_actual_publication(
    provider_app, inflight_provider_http, monkeypatch
):
    """Complete atomic publication while a genuine HTTP Reader retains its old native snapshot."""
    context, client = provider_app, inflight_provider_http
    path = _PATH + f"?network_ids={context.source_network_id},{context.seed.network['record_id']}"
    headers_by_name = {**_AUTH, "X-Network-Access-Scope": "a" * 64}
    pinned, release, snapshots = asyncio.Event(), asyncio.Event(), {}
    copy_targets, request_id, held = [], uuid4(), None
    try:
        async with asyncio.timeout(45):
            baseline = await client.get(path, headers=headers_by_name)
            assert baseline.status_code == 200
            monkeypatch.setattr(
                providers,
                "resolve_network_serving_manifest",
                _held_manifest_resolver(
                    providers.resolve_network_serving_manifest, context, pinned, release, snapshots
                ),
            )
            held = asyncio.create_task(client.get(path, headers=headers_by_name))
            await asyncio.wait_for(pinned.wait(), timeout=5)
            old_manifest = snapshots[baseline.json()["generation_id"]][0]
            assert old_manifest.schema_name == context.current_schema and not held.done()
            revision, published = await asyncio.wait_for(
                _publish_provider_replacement(context, old_manifest, request_id, copy_targets), timeout=20
            )
            assert not held.done() and not release.is_set()
            release.set()
            old_response = await asyncio.wait_for(held, timeout=5)
            fresh_response = await client.get(path, headers=headers_by_name)
            _assert_provider_cutover(context, baseline, old_response, fresh_response, published, snapshots, revision)
    finally:
        release.set()
        if held is not None:
            await asyncio.wait_for(asyncio.gather(held, return_exceptions=True), timeout=5)
        await _remove_candidates(context.fixture, copy_targets, request_id)
