# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual published provider custody resolver; complete pricing HTTP is separate."""

import json
import os
from pathlib import Path
from uuid import uuid4, uuid5

import asyncpg
import pytest

from process.network_approved_membership_source import pin_approved_membership_source
from process.network_em_provider_inputs import resolve_em_provider_inputs
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_plan_pricing_source_policy_postgres import _em_provider_native_heaps
from tests.test_registry_candidate_composition_postgres import _allow_composition_owner, _remove_candidates, _roles

pytestmark = pytest.mark.asyncio


async def _publish_fixture(fixture, schema, copy_targets, created_schema_oids_by_name):
    connection = fixture.connection
    await _allow_composition_owner(fixture)
    await connection.execute(
        "CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS intarray; CREATE EXTENSION IF NOT EXISTS btree_gin"
    )
    operator = await connection.fetchval("SELECT current_user")
    inputs = await _em_provider_native_heaps(
        connection,
        schema,
        {"owner": fixture.roles["owner"], "reader": fixture.roles["reader"], "operator": operator},
        created_schema_oids_by_name,
    )
    seed = await _seed(fixture)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    request = uuid4()
    fixture.composition_ids.append(uuid5(request, "address:" + approved.generation_id))
    candidate, addresses = await compose_registry_membership_candidate(
        connection,
        request_id=request,
        approved_revision=seed.revision,
        expected_head=0,
        address_sources=RegistryCompositionAddressSources(fixture.base, em_provider_inputs=inputs),
        writer_roles=_roles(fixture),
        control_schema=fixture.control_schema,
    )
    copy_targets.append(candidate)
    await prepare_and_publish_network_candidate(
        connection, candidate, addresses, **_roles(fixture), control_schema=fixture.control_schema
    )
    await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    for table in (
        "network_serving_manifest",
        "network_serving_control",
        "network_membership_candidate",
        "network_candidate_custody",
    ):
        await connection.execute(f'GRANT SELECT ON "{fixture.control_schema}"."{table}" TO "{fixture.roles["reader"]}"')
    return inputs, candidate


async def test_published_provider_resolver(custom_db, monkeypatch):
    """Publish through composition and verify the exact manifest with an ordinary Reader."""
    fixture = custom_db
    connection = fixture.connection
    schema = "em_resolver_" + uuid4().hex
    reader = None
    copy_targets, created_schema_oids_by_name = [], {}
    password = uuid4().hex
    try:
        inputs, candidate = await _publish_fixture(fixture, schema, copy_targets, created_schema_oids_by_name)
        await connection.execute(f"ALTER ROLE \"{fixture.roles['reader']}\" LOGIN PASSWORD '{password}'")
        dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
        reader = await asyncpg.connect(dsn, user=fixture.roles["reader"], password=password)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema)
            assert manifest.candidate_id == candidate.candidate_id
            assert "em_provider_inputs" in manifest.source_generations
            assert await resolve_em_provider_inputs(reader, manifest, fixture.control_schema) == inputs
            identity = await reader.fetchrow(
                "SELECT session_user,current_user,current_setting('transaction_read_only'),current_setting('transaction_isolation'),(SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
            )
            assert tuple(identity) == (fixture.roles["reader"], fixture.roles["reader"], "on", "repeatable read", False)
        if evidence := os.getenv("MANAGED_EM_NATIVE_EVIDENCE"):
            Path(evidence).write_text(
                json.dumps(
                    {
                        "full_published_provider_resolver": True,
                        "actual_composition_publication": True,
                        "ordinary_reader": True,
                        "repeatable_read_readonly": True,
                        "provider_relations": 9,
                        "manifest_sha256": manifest.manifest_sha256,
                        "full_pricing_release_attribution_resolution": False,
                        "http": False,
                    },
                    indent=2,
                )
                + "\n"
            )
    finally:
        if reader is not None:
            await reader.close()
        await _remove_candidates(fixture, copy_targets, uuid4())
        for namespace, oid in created_schema_oids_by_name.items():
            if await connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", namespace) == oid:
                await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
