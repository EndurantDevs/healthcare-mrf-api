# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed provider-only HTTP requests on the same protected serving transaction."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import network_providers as providers
from api.network_address_scope import current_network_address_scope
from tests.test_network_provider_only_read_postgres import cms_recipe_database as cms_recipe_database
from tests.test_network_provider_only_read_postgres import cms_recipe_template as cms_recipe_template
from tests.test_network_provider_only_read_postgres import custom_db as custom_db
from tests.test_network_provider_only_read_postgres import provider_only_db as provider_only_db
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_network_provider_only_read_postgres import serving_schema as serving_schema
from tests.test_network_provider_routes_postgres import _durable_state
from tests.test_network_serving_routes_postgres import _reader_engine

pytestmark = pytest.mark.asyncio
_AUTH = {"Authorization": "Bearer synthetic-provider-only-token"}
_PATH = "/api/v1/registry/serving/providers"


@pytest.fixture
async def provider_only_app(provider_only_db, monkeypatch):
    context = provider_only_db
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-provider-only-token")
    fixture = context.fixture
    engine = _reader_engine(fixture.engine, fixture.roles["reader"])
    sessions = async_sessionmaker(engine)
    app = Sanic("network_provider_only_http_" + uuid4().hex)
    app.blueprint(Blueprint.group([providers.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        """Keep invalid selectors from acquiring a native connection."""
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _response):
        """Release every request-owned transaction and scope."""
        await request.ctx.sa_session.close()

    try:
        yield SimpleNamespace(app=app, context=context, engine=engine)
    finally:
        await engine.dispose()


async def test_provider_mode_keeps_private_pinned_transaction_and_office_shape(provider_only_app, monkeypatch):
    application = provider_only_app
    context = application.context
    before = await _durable_state(context.fixture)
    settings = []
    native_read = providers.read_network_provider_only_page

    async def observe_read(connection, scope, **arguments):
        assert current_network_address_scope() is scope and scope.access_scope_sha256 == "a" * 64
        settings.append(
            await connection.fetchrow(
                "SELECT current_user,current_setting('transaction_isolation'),current_setting('transaction_read_only')"
            )
        )
        return await native_read(connection, scope, **arguments)

    monkeypatch.setattr(providers, "read_network_provider_only_page", observe_read)
    query = f"?network_ids={context.network_id}"
    _, page = await application.app.asgi_client.get(
        _PATH + query + "&membership_level=provider&limit=1",
        headers=_AUTH | {"X-Network-Access-Scope": "a" * 64},
    )
    assert page.status == 200 and page.json["membership_level"] == "provider" and page.json["total_count"] == 2
    assert page.json["providers"][0]["provider_id"] == "1000000004"
    assert "location" not in page.text and "pricing" not in page.text
    assert page.headers["Cache-Control"] == "private, no-store"
    assert page.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert tuple(settings[0]) == (context.fixture.roles["reader"], "repeatable read", "on")
    assert current_network_address_scope() is None
    _, implicit = await application.app.asgi_client.get(_PATH + query, headers=_AUTH)
    _, explicit = await application.app.asgi_client.get(_PATH + query + "&membership_level=office", headers=_AUTH)
    assert implicit.status == explicit.status == 200 and implicit.json == explicit.json
    assert "membership_level" not in implicit.json and implicit.json["total_count"] == 1
    assert "locations" in implicit.json["providers"][0]
    assert await _durable_state(context.fixture) == before


async def test_provider_mode_rejects_detail_and_office_service_pricing_criteria_before_database(provider_only_app):
    application = provider_only_app
    statements = []
    event.listen(
        application.engine.sync_engine, "before_cursor_execute", lambda *arguments: statements.append(arguments[2])
    )
    criterion_strings = [
        "location_id=" + str(uuid4()),
        "location_id=",
        "lat=1",
        "long=1",
        "radius_miles=1",
        "city=Example",
        "state=CA",
        "postal_code=90210",
        "service_code=123",
        "procedure_code=123",
        "price=1",
        "pricing=true",
        "membership_level=provider",
        "unknown=true",
    ]
    prefix = _PATH + f"?network_ids={application.context.network_id}&membership_level=provider"
    invalid_paths = [prefix + "&" + criterion for criterion in criterion_strings]
    invalid_paths.extend(
        [
            _PATH + "/npi/1000000491?network_ids=1&membership_level=provider",
            _PATH + "?network_ids=1&membership_level=",
            _PATH + "?network_ids=1&membership_level=Provider",
            _PATH + "?network_ids=1&membership_level=unknown",
            _PATH + "?network_ids=" + ",".join(map(str, range(1, 52))) + "&membership_level=provider",
        ]
    )
    for path in invalid_paths:
        _, rejected = await application.app.asgi_client.get(path, headers=_AUTH)
        assert rejected.status == 400 and rejected.json == {"error": {"code": "network_provider_request_invalid"}}
        assert rejected.headers["Cache-Control"] == "private, no-store"
    for headers in ({}, {"Authorization": "Bearer wrong-token"}):
        _, denied = await application.app.asgi_client.get(prefix, headers=headers)
        assert denied.status == 403 and denied.headers["Cache-Control"] == "private, no-store"
    assert statements == []


async def test_provider_mode_does_not_fabricate_coverage_for_retained_or_unknown_network(provider_only_app):
    application = provider_only_app
    context = application.context
    for query in (
        f"network_ids={context.other_network_id}",
        f"network_ids={context.network_id}&network_generation={context.original.generation_id}",
    ):
        _, unavailable = await application.app.asgi_client.get(
            _PATH + "?" + query + "&membership_level=provider", headers=_AUTH
        )
        assert unavailable.status == 503 and unavailable.json == {"error": {"code": "network_provider_unavailable"}}
        assert unavailable.headers["Cache-Control"] == "private, no-store"
