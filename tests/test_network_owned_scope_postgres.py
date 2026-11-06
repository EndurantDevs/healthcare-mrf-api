# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native API resolution retains exact geo pages and strict public DTO pins."""

from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import network_serving_read as serving
from process.network_approved_membership_source import (
    ApprovedMembershipSourceError,
    pin_api_retained_approved_membership_source,
    resolve_api_retained_approved_membership_source,
)
from process.provider_directory_projection_stage import _copy_driver
from tests.test_network_canonical_cutover_postgres import (
    _assert_nearby_page,
    _nearby_http_reply,
)
from tests.test_network_canonical_cutover_postgres import admitted_nearby_app as admitted_nearby_app
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


@pytest.mark.parametrize("retained", [False, True])
async def test_owned_geo_resolution_keeps_native_pages_and_one_original_reader(
    admitted_nearby_app, monkeypatch, retained
):
    context, calls = admitted_nearby_app, []
    original = serving.resolve_network_serving_manifest

    async def resolve(connection, **options):
        identity_values = tuple(
            await connection.fetchrow(
                "SELECT session_user=current_user,pg_backend_pid(),current_setting('transaction_isolation'),"
                "current_setting('transaction_read_only')"
            )
        )
        assert identity_values == (True, connection.get_server_pid(), "repeatable read", "on")
        calls.append((connection, identity_values, options))
        return await original(connection, **options)

    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolve)
    manifest = context.fixture.published_manifest
    query_by_name = {
        "network_ids": str(context.fixture.network_id),
        "lat": "40.75",
        "long": "-73.98",
        "include_total": "1",
        "limit": "1",
    }
    if retained:
        query_by_name["network_generation"] = str(manifest["generation_id"])
    headers_by_name = {"Authorization": "Bearer synthetic-network-nearby-token", "X-Network-Access-Scope": "a" * 64}
    for expected_npi in (1000000004, 1000000491):
        calls.clear()
        reply = await _nearby_http_reply(context, query_by_name, headers_by_name)
        _assert_nearby_page(reply, generation=manifest["generation_id"], provider_npi=expected_npi, total_count=2)
        assert len(calls) == 1
        assert calls[0][2] == {
            "generation_id": manifest["generation_id"] if retained else None,
            "control_schema": None,
            "api_read": True,
        }
        query_by_name["cursor"] = reply.json["next_cursor"]


async def test_public_dto_pin_still_refuses_forgery_after_owned_native_resolution(admitted_nearby_app):
    context = admitted_nearby_app
    sessions = async_sessionmaker(context.reader_engine)
    async with sessions() as session, session.begin():
        await session.execute(text("SELECT 1"))
        reader = await _copy_driver(session)
        manifest, source, relation = await resolve_api_retained_approved_membership_source(
            reader, control_schema=context.fixture.control_schema
        )
        assert source.generation_id == manifest.source_generations["custom_membership"]
        assert relation.table_name == "registry_approved_record"
        forged = replace(manifest, manifest_sha256="f" * 64)
        with pytest.raises(ApprovedMembershipSourceError, match="manifest differs"):
            await pin_api_retained_approved_membership_source(
                reader, forged, control_schema=context.fixture.control_schema
            )
        assert await pin_api_retained_approved_membership_source(
            reader, manifest, control_schema=context.fixture.control_schema
        ) == (source, relation)
