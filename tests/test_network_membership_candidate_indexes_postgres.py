# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native canonical candidate parity and catalogue-readiness checks."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import asyncpg
import pytest

from process.network_address_projection import project_network_address_arrays
from process.network_membership_candidate_indexes import NetworkCandidateIndexesError, prepare_network_candidate_indexes
from process.network_membership_validation import validate_network_membership_candidate
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def indexed_db(validation_db):
    fixture = validation_db
    await _project_validated(fixture)
    return fixture


async def _project_validated(fixture):
    async with fixture.connection.transaction():
        await validate_network_membership_candidate(
            fixture.connection,
            fixture.copy_target,
            fixture.address_source,
            control_schema=fixture.control_schema,
        )
        await project_network_address_arrays(
            fixture.connection,
            fixture.copy_target,
            fixture.address_source,
            control_schema=fixture.control_schema,
        )


async def _prepare(fixture, **changes):
    return await prepare_network_candidate_indexes(
        fixture.connection,
        changes.get("copy_target", fixture.copy_target),
        changes.get("address_source", fixture.address_source),
        control_schema=fixture.control_schema,
    )


async def _state(fixture):
    return await fixture.connection.fetchrow(
        f'SELECT state,index_ready,validation_json FROM "{fixture.control_schema}".network_membership_candidate'
    )


async def _assert_not_ready(fixture, original_state):
    assert await _state(fixture) == original_state
    assert original_state["state"] == "validated" and original_state["index_ready"] is False


@pytest.mark.asyncio
async def test_ready_receipt_and_exact_replay(indexed_db):
    fixture = indexed_db
    original_report = json.loads((await _state(fixture))["validation_json"])
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        retained_addresses = await fixture.observer.fetch(
            f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
        )
        async with fixture.connection.transaction():
            await asyncio.sleep(0)
            logged_queries.clear()
            receipt = await _prepare(fixture)
            await asyncio.sleep(0)
            assert len(logged_queries) == 10
        assert (
            await fixture.observer.fetch(
                f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
            )
            == retained_addresses
        )
    ready_state = await _state(fixture)
    assert ready_state["state"] == "ready" and ready_state["index_ready"] is True
    stored_report = json.loads(ready_state["validation_json"])
    assert stored_report == original_report | {"candidate_readiness": receipt}
    assert receipt["source_rows"] == receipt["address_rows"] == 4
    assert receipt["component"] == "canonical_address_projection" and all(receipt["index_checks"].values())
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        assert await _prepare(fixture) == receipt
        await asyncio.sleep(0)
        assert len(logged_queries) == 3
    assert await _state(fixture) == ready_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "array_expression",
    [
        "ARRAY[42,7]",
        "ARRAY[7,42,42]",
        "ARRAY[0,7,42]",
        "ARRAY[-1,7,42]",
        "ARRAY[7,99]",
        "'{}'::integer[]",
        "ARRAY[NULL]::integer[]",
    ],
)
async def test_tampered_arrays_are_rejected(indexed_db, array_expression):
    fixture = indexed_db
    original_state = await _state(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.copy_target.schema_name}".entity_address_unified SET canonical_network_ids={array_expression} WHERE location_key=$1',
        "a" * 64,
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="canonical arrays"):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra", "source_payload", "projected_payload"])
async def test_full_source_parity_is_required(indexed_db, damage):
    fixture = indexed_db
    original_state = await _state(fixture)
    projection_table = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    if damage == "missing":
        await fixture.connection.execute(f"DELETE FROM {projection_table} WHERE location_key=$1", "d" * 64)
    elif damage == "extra":
        await fixture.connection.execute(
            f"INSERT INTO {projection_table} SELECT $1,entity_type,entity_id,postal_address,plans_network_array,canonical_network_ids FROM {projection_table} WHERE location_key=$2",
            "e" * 64,
            "d" * 64,
        )
    else:
        address_table = (
            f'"{fixture.control_schema}".retained_addresses' if damage == "source_payload" else projection_table
        )
        await fixture.connection.execute(
            f"UPDATE {address_table} SET postal_address='changed synthetic payload' WHERE location_key=$1", "d" * 64
        )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="address rows"):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["missing_gin", "array_ops", "partial", "expression", "missing_pk", "composite_pk", "nullable", "wrong_type"],
)
async def test_actual_index_definition_is_required(indexed_db, damage):
    fixture = indexed_db
    original_state = await _state(fixture)
    await _damage_index(fixture, damage)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="GIN is not ready"):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


async def _damage_index(fixture, damage):
    namespace = f'"{fixture.copy_target.schema_name}"'
    projection_table = f"{namespace}.entity_address_unified"
    if damage in {"missing_pk", "composite_pk"}:
        await fixture.connection.execute(f"ALTER TABLE {projection_table} DROP CONSTRAINT entity_address_unified_pkey")
        if damage == "composite_pk":
            await fixture.connection.execute(f"ALTER TABLE {projection_table} ADD PRIMARY KEY(location_key,entity_id)")
        return
    if damage == "nullable":
        await fixture.connection.execute(
            f"ALTER TABLE {projection_table} ALTER COLUMN canonical_network_ids DROP NOT NULL"
        )
        await fixture.connection.execute(
            f"UPDATE {projection_table} SET canonical_network_ids=NULL WHERE location_key=$1", "d" * 64
        )
        return
    await fixture.connection.execute(f"DROP INDEX {namespace}.canonical_network_ids_gin")
    statements_by_damage = {
        "array_ops": f"CREATE INDEX canonical_network_ids_gin ON {projection_table} USING GIN(canonical_network_ids array_ops)",
        "partial": f"CREATE INDEX canonical_network_ids_gin ON {projection_table} USING GIN(canonical_network_ids gin__int_ops) WHERE cardinality(canonical_network_ids)>0",
        "expression": f"CREATE INDEX canonical_network_ids_gin ON {projection_table} USING GIN((canonical_network_ids || '{{}}'::integer[]) gin__int_ops)",
        "wrong_type": f"ALTER TABLE {projection_table} ALTER COLUMN canonical_network_ids TYPE bigint[] USING canonical_network_ids::bigint[]",
    }
    if damage in statements_by_damage:
        await fixture.connection.execute(statements_by_damage[damage])


@pytest.mark.asyncio
async def test_failed_concurrent_gin_is_not_ready(indexed_db):
    fixture = indexed_db
    original_state = await _state(fixture)
    namespace = f'"{fixture.copy_target.schema_name}"'
    projection_table = f"{namespace}.entity_address_unified"
    await fixture.connection.execute(f"DROP INDEX {namespace}.canonical_network_ids_gin")
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        await fixture.observer.fetchval(f"SELECT count(*) FROM {projection_table}")
        await fixture.connection.execute("SET statement_timeout='500ms'")
        try:
            with pytest.raises(asyncpg.QueryCanceledError):
                await fixture.connection.execute(
                    f"CREATE INDEX CONCURRENTLY canonical_network_ids_gin ON {projection_table} USING GIN(canonical_network_ids gin__int_ops)"
                )
        finally:
            await fixture.connection.execute("SET statement_timeout=0")
    index_flags = await fixture.connection.fetchrow(
        "SELECT indisvalid,indisready FROM pg_index WHERE indexrelid=to_regclass($1)",
        fixture.copy_target.schema_name + ".canonical_network_ids_gin",
    )
    assert index_flags is not None and index_flags["indisvalid"] is False
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="GIN is not ready"):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
async def test_scope_identity_and_accounting_are_rechecked(indexed_db):
    fixture = indexed_db
    original_state = await _state(fixture)
    with pytest.raises(NetworkCandidateIndexesError, match="caller-owned"):
        await _prepare(fixture)
    async with fixture.connection.transaction():
        for field in ("producer_id", "dataset_id", "schema_id"):
            with pytest.raises(NetworkCandidateIndexesError, match="ownership scope"):
                await _prepare(fixture, copy_target=replace(fixture.copy_target, **{field: str(uuid4())}))
        with pytest.raises(NetworkCandidateIndexesError, match="source generation"):
            await _prepare(fixture, address_source=replace(fixture.address_source, generation_id="stale"))
    await fixture.connection.execute(
        f'DELETE FROM "{fixture.control_schema}".network_registry_identity WHERE network_id=88'
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="relationships or accounting"):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "sealed", "published", "rejected"])
async def test_wrong_state_cannot_become_ready(indexed_db, state):
    fixture = indexed_db
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET state=$1,index_ready=true',
        state,
    )
    original_state = await _state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError, match="must be validated"):
            await _prepare(fixture)
    assert await _state(fixture) == original_state


@pytest.mark.asyncio
async def test_outer_rollback_retains_validated_state(indexed_db):
    fixture = indexed_db
    original_state = await _state(fixture)
    outer_transaction = fixture.connection.transaction()
    await outer_transaction.start()
    try:
        await _prepare(fixture)
    finally:
        await outer_transaction.rollback()
    await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
async def test_row_count_does_not_add_queries(validation_db):
    fixture = validation_db
    await fixture.connection.execute(f"""INSERT INTO "{fixture.control_schema}".retained_addresses
        SELECT lpad(to_hex(sequence_id),64,'0'),'npi','extra-'||sequence_id,'synthetic address','{{42}}'::int[],'{{999}}'::int[]
        FROM generate_series(1,20000) sequence_id""")
    await _project_validated(fixture)
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        receipt = await _prepare(fixture)
        await asyncio.sleep(0)
        assert len(logged_queries) == 10 and receipt["address_rows"] == 20004


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["batch_ledger", "custom_revision", "source_map", "binding"])
async def test_retained_validation_must_still_match(indexed_db, damage):
    fixture = indexed_db
    original_state = await _state(fixture)
    statements_by_damage = {
        "batch_ledger": f'DELETE FROM "{fixture.control_schema}".network_membership_batch',
        "custom_revision": f'UPDATE "{fixture.control_schema}".network_membership_candidate SET approved_custom_revision=1',
        "source_map": f'UPDATE "{fixture.control_schema}".network_membership_candidate SET source_generations=source_generations || \'{{"other":"new-edition"}}\'::jsonb',
        "binding": f"UPDATE \"{fixture.copy_target.schema_name}\".provider_location_binding SET entity_id='different-provider' WHERE provider_id='provider-b'",
    }
    await fixture.connection.execute(statements_by_damage[damage])
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError):
            await _prepare(fixture)
        await _assert_not_ready(fixture, original_state)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["index_checks", "parity_counts", "scope"])
async def test_cached_readiness_is_checked(indexed_db, damage):
    fixture = indexed_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    stored_report = json.loads((await _state(fixture))["validation_json"])
    readiness = stored_report["candidate_readiness"]
    if damage == "index_checks":
        readiness["index_checks"]["has_canonical_gin"] = False
    elif damage == "parity_counts":
        readiness["changed_network_arrays"] = 1
    else:
        readiness["scope"]["schema_revision"] = True
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$1::jsonb',
        json.dumps(stored_report),
    )
    damaged_state = await _state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCandidateIndexesError):
            await _prepare(fixture)
    assert await _state(fixture) == damaged_state
