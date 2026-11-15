# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real independent seals retain address/edge artifacts through rollback and rotation."""

import asyncio
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from process import network_compact_retention as retention
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_address_artifact_postgres import address_db
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _seal,
    compact_publication_db,
)


@pytest.fixture
async def sealed_compact(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, target)
    seal = await _seal(fixture, target)
    return fixture, target, seal


async def _guard(fixture, oids):
    async with fixture.connection.transaction(isolation="read_committed"):
        return await retention.require_network_compact_artifacts_unreferenced(
            fixture.connection, oids, control_schema=fixture.control_schema
        )


@pytest.mark.parametrize("state", ["ready", "published", "ineligible"])
async def test_sealed_candidates_retain_rollback_dependencies(sealed_compact, state):
    fixture, target, _ = sealed_compact
    if state != "ready":
        publication = await _publish(fixture, target)
        if state == "ineligible":
            await fixture.connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
                publication["generation_id"],
            )
    descriptor = fixture.address.descriptor
    for relation in (
        CompactAddressRelation(**descriptor["table"]),
        CompactAddressRelation(**descriptor["retained_table"]),
        CompactAddressRelation(target.schema_name, "network_office_edge"),
    ):
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await _guard(fixture, [relation])
    assert (await _guard(fixture, [CompactAddressRelation("pg_catalog", "pg_class")]))["relations"] == [
        ("pg_catalog", "pg_class")
    ]


async def test_second_check_sees_a_concurrently_committed_reference_change(sealed_compact):
    fixture, copy_target, _ = sealed_compact
    retired = CompactAddressRelation(fixture.address.schema, "retired_offices")
    table = f'"{retired.schema_name}"."{retired.table_name}"'
    await fixture.connection.execute(f"CREATE TABLE {table}(location_key text)")
    cleanup_connection = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    first_check, committed = asyncio.Event(), asyncio.Event()

    async def cleanup():
        async with cleanup_connection.transaction(isolation="read_committed"):
            expected = await retention.require_network_compact_artifacts_unreferenced(
                cleanup_connection, [retired], control_schema=fixture.control_schema
            )
            first_check.set()
            await committed.wait()
            await cleanup_connection.execute(f"LOCK TABLE ONLY {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
            await retention.require_network_compact_artifacts_unreferenced(
                cleanup_connection, [retired], control_schema=fixture.control_schema, expected_projection=expected
            )
            await cleanup_connection.execute(f"DROP TABLE {table} RESTRICT")

    operation = asyncio.create_task(cleanup())
    try:
        await asyncio.wait_for(first_check.wait(), 10)
        await _publish(fixture, copy_target)
    finally:
        committed.set()
    try:
        with pytest.raises(retention.NetworkCompactRetentionError, match="references_changed"):
            await operation
    finally:
        await cleanup_connection.close()
    assert await fixture.connection.fetchval("SELECT to_regclass($1) IS NOT NULL", table)


async def test_repeatable_read_cannot_hide_a_new_seal(compact_publication_db):
    fixture = compact_publication_db
    relation = CompactAddressRelation(fixture.address.schema, "entity_address_unified")
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(retention.NetworkCompactRetentionError, match="fresh_snapshot_required"):
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [relation], control_schema=fixture.control_schema
            )


async def test_legacy_cleanup_without_registry_stays_available(address_db):
    relation = CompactAddressRelation(address_db.schema, "entity_address_unified")
    # Without registry controls, a standalone cleanup needs no fresh snapshot.
    async with address_db.admin.transaction(isolation="repeatable_read"):
        assert await retention.require_network_compact_artifacts_unreferenced(
            address_db.admin, [relation], control_schema="absent_registry"
        ) == {
            "schema": "absent_registry",
            "relations": [(address_db.schema, "entity_address_unified")],
            "projection": None,
        }


async def _open_candidate_controls(connection, control, address_source):
    """Install the three logical controls with one open candidate admitted before validation."""
    await connection.execute(f'CREATE SCHEMA "{control}"')
    await connection.execute(f"""CREATE TABLE "{control}".network_membership_candidate(
        dataset_id uuid,schema_id uuid,producer_id uuid,candidate_id uuid PRIMARY KEY,schema_name text,state text,
        source_generations jsonb,source_recipes_json jsonb,validation_json jsonb,schema_revision integer,
        approved_custom_revision bigint,expected_head bigint,expected_rows bigint,accepted_rows bigint,index_ready boolean)""")
    await connection.execute(
        f'CREATE TABLE "{control}".network_serving_manifest(generation_id bigint,candidate_id uuid,'
        "manifest_sha256 text,eligible boolean)"
    )
    await connection.execute(
        f'CREATE TABLE "{control}".network_serving_control(id integer,generation_id bigint,metadata_generation bigint)'
    )
    await connection.execute(f'INSERT INTO "{control}".network_serving_control VALUES(1,NULL,0)')
    candidate_id = uuid4()
    report = None if address_source is None else {"address_source": address_source}
    await connection.execute(
        f'INSERT INTO "{control}".network_membership_candidate VALUES'
        "($1,$1,$1,$1,$2,'open',$3::jsonb,'[]'::jsonb,$4::jsonb,1,0,0,0,0,false)",
        candidate_id,
        "network_candidate_" + candidate_id.hex,
        json.dumps({"unified_address": "a" * 64}),
        None if report is None else json.dumps(report),
    )


async def _assert_retirement(connection, control, requested, expected):
    """Run the guard in a fresh snapshot; ``None`` means the retirement is allowed."""
    async with connection.transaction(isolation="read_committed"):
        if expected is None:
            projection = await retention.require_network_compact_artifacts_unreferenced(
                connection, [requested], control_schema=control
            )
            assert projection["relations"] == [(requested.schema_name, requested.table_name)]
            return
        with pytest.raises(retention.NetworkCompactRetentionError, match=expected):
            await retention.require_network_compact_artifacts_unreferenced(
                connection, [requested], control_schema=control
            )


@pytest.mark.parametrize("recorded", [True, False])
async def test_open_candidate_protects_only_its_admitted_address_generation(recorded):
    raw = os.environ.get("NETWORK_REGISTRY_TEST_DSN")
    if not raw:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN is not set")
    connection = await asyncpg.connect(raw.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    control, own = "retention_control_" + suffix, CompactAddressRelation("address_" + suffix, "entity_address_unified")
    unrelated = CompactAddressRelation("unrelated_" + suffix, "entity_address_unified")
    address_by_field = {"schema_name": own.schema_name, "table_name": own.table_name, "generation_id": "a" * 64}
    try:
        await _open_candidate_controls(connection, control, address_by_field if recorded else None)
        # Without a recorded binding the guard cannot tell its generation apart, so it refuses every target.
        await _assert_retirement(
            connection, control, own, "artifact_referenced" if recorded else "references_incomplete"
        )
        await _assert_retirement(connection, control, unrelated, None if recorded else "references_incomplete")
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{control}" CASCADE')
        await connection.close()
