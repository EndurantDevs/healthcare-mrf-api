# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native provider HTTP reads through approved composition and protected reader roles."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

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
