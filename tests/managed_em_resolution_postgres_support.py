# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual published provider custody resolver; complete pricing HTTP is separate."""

import json
import os
from pathlib import Path
from uuid import uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from api.network_read_execution import _installed_read_inventory, execute_network_read
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_em_provider_inputs import resolve_em_provider_inputs, resolve_em_provider_inputs_in_session
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_reactivation import (
    NetworkServingReactivationError,
    reactivate_network_serving_generation,
)
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
    await _assert_unreadable_input_keeps_head(fixture, inputs, candidate, addresses)
    await _publish_while_input_is_held(fixture, inputs, candidate, addresses)
    await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    for table in (
        "network_serving_manifest",
        "network_serving_control",
        "network_membership_candidate",
        "network_candidate_custody",
    ):
        await connection.execute(f'GRANT SELECT ON "{fixture.control_schema}"."{table}" TO "{fixture.roles["reader"]}"')
    await _assert_concurrent_removal_keeps_head(fixture, inputs, approved, copy_targets)
    return inputs, candidate


async def _assert_unreadable_input_keeps_head(fixture, inputs, candidate, addresses):
    """An input that no longer resolves, or lacks a read column, refuses publication and leaves the head unset."""
    connection, owner, npi = fixture.connection, fixture.roles["owner"], inputs.relation("npi")
    schema, table = npi.split(".")
    for change, restore in (
        (f"ALTER TABLE {npi} RENAME TO missing_input", f"ALTER TABLE {schema}.missing_input RENAME TO {table}"),
        (
            f"ALTER TABLE {npi} RENAME COLUMN entity_type_code TO entity_type",
            f"ALTER TABLE {npi} RENAME COLUMN entity_type TO entity_type_code",
        ),
    ):
        await connection.execute(f'SET ROLE "{owner}"; {change}; RESET ROLE')
        try:
            with pytest.raises(ValueError, match="^Managed provider input custody is unavailable$") as refused:
                await prepare_and_publish_network_candidate(
                    connection, candidate, addresses, **_roles(fixture), control_schema=fixture.control_schema
                )
            assert refused.value.__cause__.__cause__ is None
            head = f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            assert not await connection.fetchval(head)
        finally:
            await connection.execute(f'SET ROLE "{owner}"; {restore}; RESET ROLE')


async def _publish_while_input_is_held(fixture, inputs, candidate, addresses):
    """The head switch reads no input row and takes no input lock, so another session may hold an input exclusively."""
    connection, owner, npi = fixture.connection, fixture.roles["owner"], inputs.relation("npi")
    holder = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        async with holder.transaction():
            await holder.execute(f'SET LOCAL ROLE "{owner}"; LOCK TABLE {npi} IN ACCESS EXCLUSIVE MODE')
            await connection.execute("SET lock_timeout = '10s'")
            await prepare_and_publish_network_candidate(
                connection, candidate, addresses, **_roles(fixture), control_schema=fixture.control_schema
            )
    finally:
        await connection.execute("RESET lock_timeout")
        await holder.close()


async def _assert_concurrent_removal_keeps_head(fixture, inputs, approved, copy_targets):
    """Reactivation keeps the head when another session removes an input after its snapshot began."""
    connection, owner, npi = fixture.connection, fixture.roles["owner"], inputs.relation("npi")
    schema, table = npi.split(".")
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        first = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
    request = uuid4()
    fixture.composition_ids.append(uuid5(request, "address:" + approved.generation_id))
    retained = PinnedAddressSource(first.schema_name, "entity_address_unified", first.manifest_sha256)
    successor, addresses = await compose_registry_membership_candidate(
        connection,
        request_id=request,
        approved_revision=approved.approved_revision,
        expected_head=first.generation_id,
        source_manifest=first,
        address_sources=RegistryCompositionAddressSources(retained, em_provider_inputs=inputs),
        writer_roles=_roles(fixture),
        control_schema=fixture.control_schema,
    )
    copy_targets.append(successor)
    published = await prepare_and_publish_network_candidate(
        connection, successor, addresses, **_roles(fixture), control_schema=fixture.control_schema
    )
    arguments_by_name = dict(
        generation_id=first.generation_id,
        expected_head=published["generation_id"],
        expected_approved_revision=approved.approved_revision,
        control_schema=fixture.control_schema,
    )
    head = f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
    other = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        async with connection.transaction(isolation="repeatable_read"):
            assert await connection.fetchval(head) == published["generation_id"]
            await other.execute(f'SET ROLE "{owner}"; ALTER TABLE {npi} RENAME TO removed_input; RESET ROLE')
            with pytest.raises(NetworkServingReactivationError, match="network_reactivation_controls_unavailable"):
                await reactivate_network_serving_generation(connection, **arguments_by_name)
            assert await connection.fetchval(head) == published["generation_id"]
        await other.execute(f'SET ROLE "{owner}"; ALTER TABLE {schema}.removed_input RENAME TO {table}; RESET ROLE')
        async with connection.transaction(isolation="repeatable_read"):
            assert (await reactivate_network_serving_generation(connection, **arguments_by_name)).changed
    finally:
        await other.close()


async def _api_inputs(reader_engine, manifest, control_schema, *settings):
    async with AsyncSession(reader_engine) as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        for setting in settings:
            await session.execute(text(setting))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        return await resolve_em_provider_inputs(driver, manifest, control_schema, api_read=True)


async def _assert_api_inputs_by_name(fixture, reader_engine, manifest, inputs):
    """Admit inputs by name under a share lock; row security, a missing column or a lock wait refuse the read."""
    connection, owner, npi = fixture.connection, fixture.roles["owner"], inputs.relation("npi")
    async with AsyncSession(reader_engine) as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        assert await resolve_em_provider_inputs(driver, manifest, fixture.control_schema, api_read=True) == inputs
        # The admitting read holds its share lock, so enabling row level security waits until the read ends.
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with connection.transaction():
                await connection.execute(
                    f"SET LOCAL lock_timeout = '200ms'; SET LOCAL ROLE \"{owner}\"; "
                    f"ALTER TABLE {npi} ENABLE ROW LEVEL SECURITY"
                )
    # Under repeatable read pg_class is read through a snapshot that can predate the lock; a row filter committed
    # after that snapshot is still refused.
    try:
        async with AsyncSession(reader_engine) as session:
            with pytest.raises(PlanPricingProjectionUnavailable):
                async with session.begin():
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                    await session.execute(text("SELECT 1"))
                    await connection.execute(
                        f'SET ROLE "{owner}"; ALTER TABLE {npi} ENABLE ROW LEVEL SECURITY; RESET ROLE'
                    )
                    driver = (await (await session.connection()).get_raw_connection()).driver_connection
                    await resolve_em_provider_inputs(driver, manifest, fixture.control_schema, api_read=True)
    finally:
        await connection.execute(f'SET ROLE "{owner}"; ALTER TABLE {npi} DISABLE ROW LEVEL SECURITY; RESET ROLE')
    srid = "ALTER COLUMN the_geom TYPE geometry(MultiPolygon,{0}) USING ST_SetSRID(the_geom,{0})"
    for table, change, restore in (
        (npi, "ENABLE ROW LEVEL SECURITY", "DISABLE ROW LEVEL SECURITY"),
        (npi, "RENAME COLUMN entity_type_code TO entity_type", "RENAME COLUMN entity_type TO entity_type_code"),
        # The held ZCTA leaf inherits this column; the managed query compares it with SRID 4269 points.
        ("tiger.zcta5", srid.format(4326), srid.format(4269)),
    ):
        await connection.execute(f'SET ROLE "{owner}"; ALTER TABLE {table} {change}; RESET ROLE')
        try:
            with pytest.raises(PlanPricingProjectionUnavailable):
                await _api_inputs(reader_engine, manifest, fixture.control_schema)
        finally:
            await connection.execute(f'SET ROLE "{owner}"; ALTER TABLE {table} {restore}; RESET ROLE')
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{owner}"; LOCK TABLE {npi} IN ACCESS EXCLUSIVE MODE')
        with pytest.raises(PlanPricingProjectionUnavailable):
            await _api_inputs(reader_engine, manifest, fixture.control_schema, "SET LOCAL lock_timeout = '200ms'")


def _write_resolver_evidence(manifest):
    """Write only the established native resolver scope when explicitly requested."""
    if evidence := os.getenv("MANAGED_EM_NATIVE_EVIDENCE"):
        Path(evidence).write_text(
            json.dumps(
                {
                    "full_published_provider_resolver": True,
                    "actual_composition_publication": True,
                    "ordinary_reader": True,
                    "repeatable_read_readonly": True,
                    "provider_relations": 9,
                    "api_read_provider_resolver": True,
                    "api_original_session_nine_input_inventory": True,
                    "api_guarded_native_provider_read": True,
                    "legacy_no_seal_control_identity_only": True,
                    "manifest_sha256": manifest.manifest_sha256,
                    "full_pricing_release_attribution_resolution": False,
                    "http": False,
                },
                indent=2,
            )
            + "\n"
        )


async def test_published_provider_resolver(custom_db, monkeypatch):
    """Publish through composition and verify the exact manifest with an ordinary Reader."""
    fixture = custom_db
    connection = fixture.connection
    schema = "em_resolver_" + uuid4().hex
    reader, reader_engine = None, None
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
        reader_engine = create_async_engine(
            make_url(dsn).set(drivername="postgresql+asyncpg", username=fixture.roles["reader"], password=password),
            isolation_level="REPEATABLE READ",
        )
        async with AsyncSession(reader_engine) as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            api_manifest = await resolve_network_serving_manifest(
                driver, generation_id=manifest.generation_id, control_schema=fixture.control_schema, api_read=True
            )
            assert api_manifest == manifest
            assert not api_manifest.custody_control and api_manifest.compact_layout is None
            assert await resolve_em_provider_inputs_in_session(session, api_manifest, fixture.control_schema) == inputs
            inventory = _installed_read_inventory(session)
            assert inventory.connection is driver and len(inventory.relations) == 9
            provider_result = await execute_network_read(
                session, f"SELECT npi FROM ONLY {inputs.relation('npi')} LIMIT 1"
            )
            assert len(provider_result.all()) <= 1
            api_identity = await driver.fetchrow(
                "SELECT session_user,current_user,current_setting('transaction_read_only'),"
                "current_setting('transaction_isolation'),(SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
            )
            assert tuple(api_identity) == tuple(identity)
        await _assert_api_inputs_by_name(fixture, reader_engine, api_manifest, inputs)
        _write_resolver_evidence(manifest)
    finally:
        if reader_engine is not None:
            await reader_engine.dispose()
        if reader is not None:
            await reader.close()
        await _remove_candidates(fixture, copy_targets, uuid4())
        for namespace, oid in created_schema_oids_by_name.items():
            if await connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", namespace) == oid:
                await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
