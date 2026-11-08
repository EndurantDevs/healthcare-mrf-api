# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native owned pools and current protected EAU observation; App paging is a transport double."""

from contextlib import AsyncExitStack
from types import SimpleNamespace

import httpx
import pytest

from db import models
from process import network_address_artifact as address
from process import registry_membership_refresh_runtime as runtime
from tests.test_network_address_artifact_postgres import address_db as address_db
from tests.test_registry_membership_refresh_queue_postgres import refresh_queue_db as refresh_queue_db
from tests.test_registry_membership_refresh_runtime import _reply


async def _require_approved_snapshot(fixture):
    """Require the approved-record tables already installed by the complete fixture chain."""
    assert await fixture.admin.fetchval(
        "SELECT count(*)=2 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) AND c.relkind='r'",
        fixture.schema,
        ["registry_approved_record", "registry_approval_history"],
    )
    await fixture.admin.execute(
        f'GRANT SELECT ON "{fixture.schema}".registry_approved_record TO "{fixture.source.roles["reader"]}"'
    )


@pytest.mark.asyncio
async def test_native_api_bundle_owns_distinct_pools_and_observes_address_authority(refresh_queue_db, monkeypatch):
    fixture = refresh_queue_db
    await _require_approved_snapshot(fixture)
    address_fixture = fixture.source
    reader_role = address_fixture.roles["reader"]
    await address_fixture.admin.execute(f'ALTER ROLE "{reader_role}" SET temp_file_limit=262144')
    url = address_fixture.reader_url.set(drivername="postgresql+asyncpg")
    configured_by_field = {
        "enabled": True,
        "app_origin": "https://authority.example",
        "service_token": "synthetic-dedicated-token",
        "control_schema": fixture.schema,
        "fence_dsn": url.render_as_string(hide_password=False),
        "source_reader_dsn": url.render_as_string(hide_password=False),
        "address_reader_dsn": url.render_as_string(hide_password=False),
        "address_schema": address_fixture.schema,
        "runtime_roles": [reader_role],
        "source_profiles": {},
    }

    async def api_rows(statement, **parameters):
        return [
            tuple(row.values())
            for row in await address._CatalogSession(address_fixture.reader).execute(statement, parameters)
        ]

    monkeypatch.setattr(models, "db", SimpleNamespace(all=api_rows))
    async with AsyncExitStack() as cleanup:
        bundle = await runtime.build_registry_membership_refresh_runtime(configured_by_field, cleanup)
        resources = bundle.inputs_service.resources
        factories = (resources.fence_sessions, resources.source_sessions, resources.address_sessions)
        assert len({id(factory.kw["bind"].pool) for factory in factories}) == 3
        authority = bundle.runtime.authorize
        await authority._client.aclose()
        authority._client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: _reply({"policy_revision": 1, "enrolled_scopes": [], "next_binding_id": None})
            )
        )
        observation = await bundle.inputs_service()
        assert observation["accepted_sources"] == observation["enrolled_scopes"] == []
        assert observation["address_generation"] == address_fixture.source.generation_id
        assert observation["address_artifact_sha256"] == address_fixture.descriptor["descriptor_sha256"]
        assert observation["ready_address_generation"] is None and observation["expected_serving_generation"] == 0
        assert observation["expected_serving_manifest_sha256"] is None
    assert all(factory.kw["bind"].pool.checkedout() == 0 for factory in factories)
