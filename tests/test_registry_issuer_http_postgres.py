# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issuer HTTP batches read source-period evidence through actual native storage."""

from functools import partial
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import issuer
from process.registry_issuer_resolution import read_registry_issuer_resolutions
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_issuer_resolution_postgres import _company_group, _evidence, _source

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/issuer/registry"


@pytest.fixture
async def issuer_app(serving_schema, monkeypatch):
    """Inject only the task-owned namespace into the real evidence reader."""
    connection, schema, source_engine = serving_schema
    monkeypatch.setattr(
        issuer, "read_registry_issuer_resolutions", partial(read_registry_issuer_resolutions, control_schema=schema)
    )
    engine = create_async_engine(source_engine.url)
    sessions = async_sessionmaker(engine)
    app = Sanic("registry_issuer_http_" + uuid4().hex)
    app.blueprint(Blueprint.group([issuer.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        """Open the lazy transaction without touching source data."""
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _reply):
        """Release every request-owned native session on both outcomes."""
        await request.ctx.sa_session.close()

    try:
        yield SimpleNamespace(app=app, connection=connection, schema=schema)
    finally:
        await engine.dispose()


async def test_http_issuer_batch_preserves_integer_contract_and_period(issuer_app):
    fixture = issuer_app
    company_id, group_id = await _company_group(fixture.connection, fixture.schema)
    old = await _source(fixture.connection, fixture.schema, year=2024)
    await _evidence(fixture.connection, fixture.schema, old, company_id, group_id)
    newest = await _source(fixture.connection, fixture.schema, year=2025)
    await _evidence(fixture.connection, fixture.schema, newest, None, None, status="unresolved")
    _, latest = await fixture.app.asgi_client.get(_PATH + "?issuer_ids=123,00456")
    assert latest.status == 200 and latest.headers["Cache-Control"] == "private, no-store"
    records = latest.json["issuers"]
    assert [record["issuer_id"] for record in records] == [123, 456]
    assert records[0]["hios_issuer_id"] == "00123" and records[0]["reporting_year"] == 2025
    assert records[0]["legal_company"] is None and records[0]["company_resolution_status"] == "unresolved"
    assert records[1]["resolution_status"] == "missing"
    _, historical = await fixture.app.asgi_client.get(_PATH + "?issuer_ids=00123&reporting_year=2024")
    assert historical.status == 200
    record = historical.json["issuers"][0]
    assert record["relationship_scope"] == "source_reporting_period"
    assert record["legal_company"] == {"company_id": str(company_id), "source_labels": ["Source legal company"]}
    assert record["reported_group"]["group_id"] == str(group_id)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?issuer_ids=",
        "?issuer_ids=0",
        "?issuer_ids=00000",
        "?issuer_ids=123,00123",
        "?issuer_ids=0123",
        "?issuer_ids=-1",
        "?issuer_ids=true",
        "?issuer_ids=123&issuer_ids=456",
        "?issuer_ids=123&reporting_year=2024&reporting_year=2025",
        "?issuer_ids=123&reporting_year=2009",
        "?issuer_ids=123&reporting_year=02024",
        "?issuer_ids=123&state=CA",
        "?issuer_ids=123&control_schema=caller",
    ],
)
async def test_http_issuer_query_rejects_untrusted_selectors(issuer_app, query):
    _, reply = await issuer_app.app.asgi_client.get(_PATH + query)
    assert reply.status == 400 and reply.json == {"error": {"code": "issuer_registry_request_invalid"}}
    assert reply.headers["Cache-Control"] == "private, no-store"


async def test_http_issuer_unavailable_storage_has_no_legacy_fallback(issuer_app):
    await issuer_app.connection.execute(f'DROP TABLE "{issuer_app.schema}".registry_issuer_company_assertion')
    _, reply = await issuer_app.app.asgi_client.get(_PATH + "?issuer_ids=123")
    assert reply.status == 503 and reply.json == {"error": {"code": "issuer_registry_unavailable"}}
