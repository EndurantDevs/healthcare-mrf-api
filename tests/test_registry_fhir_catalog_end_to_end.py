# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A real admitted import keeps the searchable catalog and retained serving data."""

import hashlib
import json
import os
import subprocess
from contextlib import asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from aiohttp import web
from sanic import Blueprint, Sanic
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import network_catalog
from api.network_address_scope import network_address_read_scope
from api.network_provider_read import read_network_provider_page
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_catalog_policy import POLICY_V2
from process.registry_fhir_catalog_worker import run_fhir_catalog_sync
from tests.fhir_catalog_sync_integration_support import adopt_existing_network, import_source, native_catalog_case
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP


@asynccontextmanager
async def core_server(case):
    app = Sanic("catalog_integration_" + uuid4().hex)
    app.blueprint(Blueprint.group([network_catalog.blueprint], version_prefix="/api/v"))
    network_catalog.register_network_catalog_private_responses(app)
    engine = create_async_engine(case.reader_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    @app.middleware("request")
    async def open_session(request):
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _response):
        if hasattr(request.ctx, "sa_session"):
            await request.ctx.sa_session.close()

    async def forward(request):
        _, response = await app.asgi_client.post(request.path, data=await request.read(), headers=request.headers)
        return web.Response(status=response.status, body=response.body, headers=dict(response.headers))

    bridge = web.Application()
    bridge.router.add_post("/{path:.*}", forward)
    runner = web.AppRunner(bridge)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield app, "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
    finally:
        await runner.cleanup()
        await engine.dispose()


def policy(query):
    policy_by_field = {
        "contract_version": POLICY_V2,
        "client_id": "client_42",
        "policy_revision": 9,
        "organization_visibility_default": "system_wide",
        "excluded_network_ids": [],
        "excluded_company_ids": [],
        "excluded_group_ids": [],
    }
    policy_by_field["access_scope_sha256"] = hashlib.sha256(
        json.dumps(
            policy_by_field,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return {**policy_by_field, "policy_revision": "9", "query": query}


async def gateway_probe(url, network_id, status):
    command = os.getenv("NETWORK_CATALOG_GATEWAY_TEST_COMMAND")
    if command is None:
        return
    import asyncio

    environment_by_name = {
        **os.environ,
        "NETWORK_CATALOG_TEST_CORE_URL": url,
        "NETWORK_CATALOG_TEST_NETWORK_ID": str(network_id),
        "NETWORK_CATALOG_TEST_STATUS": status,
    }
    result = await asyncio.to_thread(
        subprocess.run, json.loads(command), env=environment_by_name, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr


async def serving_page(case, network_id):
    connection = await asyncpg.connect(
        case.reader_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
            with network_address_read_scope(manifest, (network_id,)) as scope:
                return await read_network_provider_page(connection, scope, control_schema=case.fixture.control_schema)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_import_sync_query_replay_and_disappearance(monkeypatch, tmp_path):
    if not os.getenv("NETWORK_REGISTRY_TEST_DSN"):
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an owned PostgreSQL fixture")
    pytest.importorskip("ptg2_address_canon")
    async with native_catalog_case(monkeypatch, tmp_path) as case:
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", case.fixture.control_schema)
        monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", "synthetic-catalog-control-token")
        await import_source(case, tmp_path / "first", retain_old=True, revision="one")
        await adopt_existing_network(case)
        old_id = int(case.seed.network["record_id"])
        first = await run_fhir_catalog_sync(case.config)
        assert first["state"] == "completed"
        async with core_server(case) as (app, url):
            query_by_field = {"view": "fhir-plans-v1", "company_id": COMPANY, "plan_search": "Gold"}

            async def read():
                _, response = await app.asgi_client.post(
                    "/api/v1/registry/serving/catalog",
                    json=policy(query_by_field),
                    headers={"Authorization": "Bearer synthetic-catalog-control-token"},
                )
                assert response.status == 200, response.json
                return response.json

            initial = await read()
            assert initial["total"] == 2
            ids = {catalog_item["network_id"] for catalog_item in initial["items"]}
            assert old_id in ids and len(ids) == 2
            old = next(catalog_item for catalog_item in initial["items"] if catalog_item["network_id"] == old_id)
            assert old["directory_available"] is True
            providers = await serving_page(case, old_id)
            assert providers["total_count"] == 1
            assert providers["providers"][0]["locations"][0]["second_line"] == "Suite 2"
            assert any(
                relationship["group"] and relationship["group"]["group_id"] == GROUP
                for relationship in old["matching_relationships"]["items"]
            )
            await gateway_probe(url, old_id, "current")
            replay = await run_fhir_catalog_sync(case.config)
            assert replay["state"] == "unchanged"
            assert await read() == initial
            await import_source(case, tmp_path / "second", retain_old=False, revision="two")
            second = await run_fhir_catalog_sync(case.config)
            assert second["state"] == "completed"
            final = await read()
            assert {catalog_item["network_id"] for catalog_item in final["items"]} == ids
            retained = next(catalog_item for catalog_item in final["items"] if catalog_item["network_id"] == old_id)
            assert retained["directory_available"] is True and retained["archived"] is False
            assert retained["source_bindings"]["items"][0]["status"] == "last_good"
            assert retained["matching_relationships"] == old["matching_relationships"]
            assert final["generation"] == initial["generation"]
            assert await serving_page(case, old_id) == providers
            await gateway_probe(url, old_id, "last_good")
