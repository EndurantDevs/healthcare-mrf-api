# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected office selection uses actual retained generations and native SQL."""

from functools import partial
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import registry_management as management
from process.registry_source_site_catalog import read_registry_source_sites
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_retained_site_adoption_postgres import retained_db

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/registry/manage/source-sites"
_AUTH = {"Authorization": "Bearer synthetic-office-control-token"}


@pytest.fixture
async def office_app(retained_db, monkeypatch):
    """Each request has its own real transaction; only fixture namespace is injected."""
    fixture = retained_db
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-office-control-token")
    monkeypatch.setattr(
        management,
        "read_registry_source_sites",
        partial(read_registry_source_sites, control_schema=fixture.control_schema),
    )
    engine = create_async_engine(fixture.engine.url)
    sessions = async_sessionmaker(engine)
    app = Sanic("registry_source_offices_" + uuid4().hex)
    app.blueprint(Blueprint.group([management.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        """Open a lazy request session without any pre-authorization database access."""
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _reply):
        """Close the request transaction and native connection after success or failure."""
        await request.ctx.sa_session.close()

    try:
        yield SimpleNamespace(app=app, fixture=fixture)
    finally:
        await engine.dispose()


def _query(fixture, **changes):
    return {
        "source_generation": fixture.source.generation_id,
        "provider_system": fixture.rows[0]["provider_system"],
        "provider_id": fixture.rows[0]["provider_id"],
        "limit": 1,
        "offset": 0,
        **changes,
    }


async def test_http_offices_keep_exact_hash_fields_and_page(office_app):
    fixture = office_app.fixture
    expected_sites = await fixture.connection.fetch(
        f'''SELECT binding.location_id::text,binding.location_key,binding.provider_system,binding.provider_id,
        encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
        FROM "{fixture.source.schema_name}".provider_location_binding binding
        JOIN "{fixture.source.schema_name}".entity_address_unified address
        ON (address.location_key,address.entity_type,address.entity_id)
        =(binding.location_key,binding.entity_type,binding.entity_id)
        WHERE binding.provider_system=$1 AND binding.provider_id=$2 ORDER BY binding.location_id''',
        fixture.rows[0]["provider_system"],
        fixture.rows[0]["provider_id"],
    )
    _, first = await office_app.app.asgi_client.post(_PATH, headers=_AUTH, json=_query(fixture))
    assert first.status == 200 and first.headers["Cache-Control"] == "private, no-store"
    assert first.json["source_generation"] == fixture.source.generation_id
    assert first.json["has_more"] is True and len(first.json["sites"]) == 1
    assert first.json["sites"][0]["fields"] == {
        "source_generation": fixture.source.generation_id,
        **dict(expected_sites[0]),
    }
    _, second = await office_app.app.asgi_client.post(_PATH, headers=_AUTH, json=_query(fixture, offset=1))
    assert second.status == 200 and second.json["has_more"] is (len(expected_sites) > 2)
    assert second.json["sites"][0]["fields"] == {
        "source_generation": fixture.source.generation_id,
        **dict(expected_sites[1]),
    }
    assert second.json["sites"][0]["fields"]["location_id"] != first.json["sites"][0]["fields"]["location_id"]
    assert set(first.json["sites"][0]["display"]) == {
        "entity_name",
        "first_line",
        "second_line",
        "city_name",
        "state_name",
        "postal_code",
        "country_code",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"source_generation": 0},
        {"source_generation": True},
        {"source_generation": "1"},
        {"provider_system": "manual"},
        {"provider_id": ""},
        {"provider_id": "bad\nidentity"},
        {"limit": 101},
        {"offset": -1},
        {"control_schema": "caller_selected"},
        {"source_receipt_json": {}},
    ],
)
async def test_http_malformed_office_query_returns_sanitized_error(office_app, changes):
    _, reply = await office_app.app.asgi_client.post(_PATH, headers=_AUTH, json=_query(office_app.fixture, **changes))
    assert reply.status == 400 and reply.json == {"error": {"code": "registry_request_invalid"}}
    assert reply.headers["Cache-Control"] == "private, no-store"


async def test_http_ineligible_generation_has_no_live_fallback(office_app):
    fixture = office_app.fixture
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
        fixture.source.generation_id,
    )
    _, reply = await office_app.app.asgi_client.post(_PATH, headers=_AUTH, json=_query(fixture))
    assert reply.status == 503 and reply.json == {"error": {"code": "registry_unavailable"}}


async def test_http_office_authorization_precedes_read(office_app, monkeypatch):
    async def forbidden_read(*_args, **_kwargs):
        pytest.fail("unauthorized office request entered native catalog")

    monkeypatch.setattr(management, "read_registry_source_sites", forbidden_read)
    _, reply = await office_app.app.asgi_client.post(_PATH, json=_query(office_app.fixture))
    assert reply.status in {401, 403}
