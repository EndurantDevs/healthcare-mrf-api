# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native canonical candidate parity and catalogue-readiness checks."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import asyncpg
import pytest

from process import network_membership_candidate_indexes as indexes
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
            assert len([entry for entry in logged_queries if "WITH source AS (" in entry.query]) == 1
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
async def test_row_count_adds_bounded_parity_batches(validation_db):
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
        assert receipt["address_rows"] == 20004
        assert len([entry for entry in logged_queries if "WITH source AS (" in entry.query]) == 5
        assert len(logged_queries) < 50


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


async def _original_parity_counts(connection, projection_table, address_table, membership_table, binding_table):
    """Retain the original unbounded expression as the independent native result oracle."""
    return await connection.fetchrow(f"""
        WITH expected_networks AS (
            SELECT binding.location_key,binding.entity_type,binding.entity_id,
                array_agg(DISTINCT membership.network_id ORDER BY membership.network_id) AS network_ids
            FROM {membership_table} membership JOIN {binding_table} binding
              ON binding.provider_system=membership.provider_system AND binding.provider_id=membership.provider_id
              AND binding.location_id=membership.location_id
            GROUP BY binding.location_key,binding.entity_type,binding.entity_id
        )
        SELECT count(source.location_key) AS source_rows, count(projected.location_key) AS address_rows,
            count(*) FILTER (WHERE source.location_key IS NOT NULL AND projected.location_key IS NULL) AS missing_locations,
            count(*) FILTER (WHERE source.location_key IS NULL) AS extra_locations,
            count(*) FILTER (WHERE source.location_key IS NOT NULL AND projected.location_key IS NOT NULL
                AND (to_jsonb(source)-'canonical_network_ids') IS DISTINCT FROM
                    (to_jsonb(projected)-'canonical_network_ids')) AS changed_address_rows,
            count(*) FILTER (WHERE projected.location_key IS NOT NULL AND projected.canonical_network_ids
                IS DISTINCT FROM COALESCE(expected.network_ids,'{{}}'::integer[])) AS changed_network_arrays
        FROM {address_table} source FULL JOIN {projection_table} projected ON projected.location_key=source.location_key
        LEFT JOIN expected_networks expected ON expected.location_key=source.location_key
            AND expected.entity_type=source.entity_type AND expected.entity_id=source.entity_id
    """)


async def _assert_batched_parity(fixture, monkeypatch):
    """Every native range must sum to the original full join, including failures."""
    projection_table = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    address_table = f'"{fixture.control_schema}".retained_addresses'
    membership_table = f'"{fixture.copy_target.schema_name}".network_membership'
    binding_table = f'"{fixture.copy_target.schema_name}".provider_location_binding'
    arguments = (fixture.connection, projection_table, address_table, membership_table, binding_table)
    expected_by_field = dict(await _original_parity_counts(*arguments))
    observed_batches = []
    original_range_counts = indexes._projection_range_counts

    async def retain_range_counts(*parameters):
        counts = await original_range_counts(*parameters)
        observed_batches.append((parameters[-2], parameters[-1], dict(counts)))
        return counts

    monkeypatch.setattr(indexes, "_projection_range_counts", retain_range_counts)
    if any(expected_by_field[field] for field in indexes._PARITY_FIELDS[2:]):
        with pytest.raises(NetworkCandidateIndexesError, match="address rows or exact canonical arrays"):
            await indexes._projection_parity(*arguments)
    else:
        assert await indexes._projection_parity(*arguments) == expected_by_field
    assert {
        field: sum(counts[field] for _, _, counts in observed_batches) for field in expected_by_field
    } == expected_by_field
    for lower_key, upper_key, _ in observed_batches:
        predicate, parameters = (
            ("location_key<=$1", (upper_key,))
            if lower_key is None
            else ("location_key>$1 AND location_key<=$2", (lower_key, upper_key))
        )
        count = await fixture.connection.fetchval(
            f"""SELECT count(*) FROM (
            SELECT location_key FROM {address_table} WHERE {predicate}
            UNION SELECT location_key FROM {projection_table} WHERE {predicate}
        ) distinct_keys""",
            *parameters,
        )
        assert 1 <= count <= 4096
    assert (
        await fixture.connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'network_parity_expected_%'"
        )
        == 0
    )


async def _damage_batched_parity(fixture, damage):
    """Exercise batch boundaries, both FULL JOIN directions and payload/array integrity."""
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    source = f'"{fixture.control_schema}".retained_addresses'
    boundary_key = f"{4096:064x}"
    if damage in {"source_empty", "both_empty"}:
        await fixture.connection.execute(f"DELETE FROM {source}")
    if damage == "both_empty":
        await fixture.connection.execute(f"DELETE FROM {projected}")
    statements_by_damage = {
        "missing": f"DELETE FROM {projected} WHERE location_key='{boundary_key}'",
        "payload": f"UPDATE {projected} SET postal_address='different full JSON payload' WHERE location_key='{boundary_key}'",
        "arrays": f"UPDATE {projected} SET canonical_network_ids=ARRAY[99,7,7] WHERE location_key IN ('{boundary_key}','{4097:064x}')",
        "disjoint_sides": f"UPDATE {projected} SET location_key='1'||substr(location_key,2) WHERE location_key<'a'",
        "extra_tail": f"INSERT INTO {projected} SELECT repeat('f',64),entity_type,entity_id,postal_address,plans_network_array,canonical_network_ids FROM {projected} WHERE location_key=repeat('a',64)",
        "extra_prefix": f"INSERT INTO {projected} SELECT repeat('0',64),entity_type,entity_id,postal_address,plans_network_array,canonical_network_ids FROM {projected} WHERE location_key=repeat('a',64)",
    }
    if damage in statements_by_damage:
        await fixture.connection.execute(statements_by_damage[damage])


@pytest.mark.asyncio
@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize(
    "damage",
    [
        "none",
        "missing",
        "payload",
        "arrays",
        "disjoint_sides",
        "extra_tail",
        "extra_prefix",
        "source_empty",
        "both_empty",
    ],
)
async def test_batched_full_join_matches_original(validation_db, monkeypatch, damage, readonly):
    fixture = validation_db
    await fixture.connection.execute(f"""INSERT INTO "{fixture.control_schema}".retained_addresses
        SELECT lpad(to_hex(sequence_id),64,'0'),'npi','batch-provider-'||sequence_id,
            'synthetic complete payload','{{42}}'::int[],'{{999}}'::int[]
        FROM generate_series(1,4097) sequence_id""")
    await _project_validated(fixture)
    await _damage_batched_parity(fixture, damage)
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=readonly):
        assert await fixture.connection.fetchval("SHOW transaction_read_only") == ("on" if readonly else "off")
        await _assert_batched_parity(fixture, monkeypatch)
        await asyncio.sleep(0)
        materialized_queries = [entry for entry in logged_queries if entry.query.startswith("CREATE TEMP TABLE")]
        assert bool(materialized_queries) is not readonly
        assert await fixture.connection.fetchval("SHOW transaction_read_only") == ("on" if readonly else "off")


@pytest.mark.asyncio
async def test_binary_equality_skips_json(indexed_db, monkeypatch):
    fixture = indexed_db
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    source = f'"{fixture.control_schema}".retained_addresses'
    predicate = await indexes._projection_address_difference(fixture.connection, projected, source)
    json_difference = (
        "(to_jsonb(source)-'canonical_network_ids') IS DISTINCT FROM (to_jsonb(projected)-'canonical_network_ids')"
    )
    assert predicate.startswith("CASE WHEN pg_catalog.record_send(ROW(")
    assert predicate.endswith(f"THEN false ELSE {json_difference} END")
    # A row-dependent division by zero proves the unequal branch is not evaluated.
    poisoned_fallback = predicate.replace(json_difference, "1/(length(source.location_key)-64)=0")
    assert await fixture.connection.fetchval(
        f"SELECT bool_and(NOT ({poisoned_fallback})) FROM {source} source "
        f"JOIN {projected} projected USING(location_key)"
    )
    async with fixture.connection.transaction():
        await _assert_batched_parity(fixture, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_copy_array_bitmap_uses_json(validation_db, monkeypatch, changed):
    fixture = validation_db
    source = f'"{fixture.control_schema}".retained_addresses'
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    await fixture.connection.execute(f"UPDATE {source} SET plans_network_array=ARRAY[42,NULL]")
    await fixture.connection.execute(f"UPDATE {source} SET plans_network_array[2]=7")
    await _project_validated(fixture)
    assert await fixture.connection.fetchval(
        f"""SELECT bool_and(array_send(source.plans_network_array)<>array_send(projected.plans_network_array)
            AND to_jsonb(source.plans_network_array)=to_jsonb(projected.plans_network_array))
        FROM {source} source JOIN {projected} projected USING(location_key)"""
    )
    if changed:
        await fixture.connection.execute(
            f"UPDATE {projected} SET plans_network_array[2]=99 WHERE location_key=$1", "a" * 64
        )
    async with fixture.connection.transaction():
        await _assert_batched_parity(fixture, monkeypatch)
        if not changed:
            assert (await _prepare(fixture))["changed_address_rows"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize("shape", ["extra_null", "renamed", "missing", "type_drift", "reordered"])
async def test_column_alignment_preserves_json(indexed_db, monkeypatch, shape, readonly):
    fixture = indexed_db
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    source = f'"{fixture.control_schema}".retained_addresses'
    statements_by_shape = {
        "extra_null": "ADD COLUMN extra_value text",
        "renamed": "RENAME COLUMN postal_address TO different_name",
        "missing": "DROP COLUMN postal_address",
        "type_drift": "ALTER COLUMN postal_address TYPE varchar USING postal_address::varchar",
        "reordered": "DROP COLUMN postal_address",
    }
    await fixture.connection.execute(f"ALTER TABLE {projected} {statements_by_shape[shape]}")
    if shape == "reordered":
        await fixture.connection.execute(f"ALTER TABLE {projected} ADD COLUMN postal_address text")
        await fixture.connection.execute(
            f"UPDATE {projected} projected SET postal_address=source.postal_address "
            f"FROM {source} source WHERE source.location_key=projected.location_key"
        )
    predicate = await indexes._projection_address_difference(fixture.connection, projected, source)
    assert ("record_send" in predicate) is (shape == "reordered")
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=readonly):
        await _assert_batched_parity(fixture, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize("unsupported", ["jsonb", "domain"])
async def test_unaudited_types_use_json(validation_db, monkeypatch, unsupported, readonly):
    fixture = validation_db
    source = f'"{fixture.control_schema}".retained_addresses'
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    type_sql = "jsonb"
    if unsupported == "domain":
        type_sql = f'"{fixture.control_schema}".int4'
        await fixture.connection.execute(f"CREATE DOMAIN {type_sql} AS integer")
    await fixture.connection.execute(f"ALTER TABLE {source} ADD COLUMN unaudited_value {type_sql}")
    await fixture.connection.execute(f"UPDATE {source} SET unaudited_value='1'")
    await _project_validated(fixture)
    predicate = await indexes._projection_address_difference(fixture.connection, projected, source)
    assert "record_send" not in predicate
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=readonly):
        await _assert_batched_parity(fixture, monkeypatch)
    await fixture.connection.execute(f"UPDATE {projected} SET unaudited_value='2' WHERE location_key=$1", "a" * 64)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=readonly):
        await _assert_batched_parity(fixture, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "type_sql,original_expression,equivalent_expression,changed_expression",
    [
        ("boolean", "true", "true", "false"),
        ("smallint", "32767", "32767", "-32768"),
        ("integer", "2147483647", "2147483647", "-2147483648"),
        ("bigint", "9223372036854775807", "9223372036854775807", "-9223372036854775808"),
        ("double precision", "'-0'", "'0'", "'Infinity'"),
        ("double precision", "'NaN'", "'NaN'", "'-Infinity'"),
        ("numeric", "1.0", "1.00", "2"),
        ("numeric(11,8)", "'NaN'", "'NaN'", "0.00000001"),
        ("uuid", "'00000000-0000-0000-0000-000000000001'", "'00000000-0000-0000-0000-000000000001'", "NULL"),
        ("date", "'-infinity'", "'-infinity'", "'infinity'"),
        ("timestamp", "'0001-01-01 BC'", "'0001-01-01 BC'", "'infinity'"),
        ("text", "NULL", "NULL", "''"),
        ("varchar(64)", "E'Åß café é\\nline '", "E'Åß café é\\nline '", "'changed'"),
        ("varchar[]", "'[0:1]={a,b}'", "ARRAY['a','b']", "ARRAY['changed']"),
        ("integer[]", "'{{1,2},{3,4}}'", "'{{1,2},{3,4}}'", "ARRAY[9]"),
    ],
)
async def test_audited_types_match_json(
    validation_db, monkeypatch, type_sql, original_expression, equivalent_expression, changed_expression
):
    fixture = validation_db
    source = f'"{fixture.control_schema}".retained_addresses'
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    await fixture.connection.execute(f"ALTER TABLE {source} ADD COLUMN parity_value {type_sql}")
    await fixture.connection.execute(f"UPDATE {source} SET parity_value={original_expression}")
    await _project_validated(fixture)
    assert "record_send" in await indexes._projection_address_difference(fixture.connection, projected, source)
    async with fixture.connection.transaction():
        await _assert_batched_parity(fixture, monkeypatch)
        await fixture.connection.execute(f"UPDATE {projected} SET parity_value={equivalent_expression}")
        await _assert_batched_parity(fixture, monkeypatch)
        await fixture.connection.execute(
            f"UPDATE {projected} SET parity_value={changed_expression} WHERE location_key=$1", "a" * 64
        )
        await _assert_batched_parity(fixture, monkeypatch)


@pytest.mark.asyncio
async def test_nondeterministic_collation_keeps_json(validation_db, monkeypatch):
    fixture = validation_db
    source = f'"{fixture.control_schema}".retained_addresses'
    projected = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    collation = f'"{fixture.control_schema}".case_insensitive'
    await fixture.connection.execute(
        f"CREATE COLLATION {collation} (provider=icu,locale='und-u-ks-level2',deterministic=false)"
    )
    await fixture.connection.execute(f"ALTER TABLE {source} ALTER COLUMN postal_address TYPE text COLLATE {collation}")
    await _project_validated(fixture)
    await fixture.connection.execute(f"UPDATE {projected} SET postal_address=upper(postal_address)")
    assert await fixture.connection.fetchval(
        f"SELECT bool_and(source.postal_address=projected.postal_address) "
        f"FROM {source} source JOIN {projected} projected USING(location_key)"
    )
    async with fixture.connection.transaction():
        await _assert_batched_parity(fixture, monkeypatch)
