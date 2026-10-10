# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native sealed set validation, immutable replay and rollback checks."""

import asyncio
import json
from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.network_membership_candidate_lifecycle import _create_raw_membership
from process.network_membership_validation import (
    NetworkMembershipValidationError,
    validate_network_membership_candidate,
)
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def validation_db(projection_db):
    fixture = projection_db
    await _native_raw_table(fixture)
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_identity(network_id,allocation_key) '
        "OVERRIDING SYSTEM VALUE VALUES(7,$1),(42,$2),(88,$3)",
        uuid4(),
        uuid4(),
        uuid4(),
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_record(network_id,display_name,archived) '
        "VALUES(42,'Synthetic archived management draft',true)"
    )
    await _batch(fixture, 2)
    await _batch(fixture, 2)
    return fixture


async def _native_raw_table(fixture):
    candidate_schema = fixture.copy_target.schema_name
    membership_records = await fixture.connection.fetch(f'SELECT * FROM "{candidate_schema}".network_membership')
    binding_records = await fixture.connection.fetch(f'SELECT * FROM "{candidate_schema}".provider_location_binding')
    await fixture.connection.execute(f'DROP SCHEMA "{candidate_schema}" CASCADE')
    await _create_raw_membership(fixture.connection, fixture.copy_target)
    await fixture.connection.execute(f"""CREATE TABLE "{candidate_schema}".provider_location_binding(
        provider_system text NOT NULL, provider_id text NOT NULL, location_id uuid NOT NULL,
        location_key varchar(64) NOT NULL, entity_type text NOT NULL, entity_id text NOT NULL,
        UNIQUE(provider_system,provider_id,location_id))
    """)
    await fixture.connection.copy_records_to_table(
        "network_membership", schema_name=candidate_schema, records=[tuple(entry) for entry in membership_records]
    )
    await fixture.connection.copy_records_to_table(
        "provider_location_binding", schema_name=candidate_schema, records=[tuple(entry) for entry in binding_records]
    )


async def _batch(fixture, row_count):
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_membership_batch '
        "(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) VALUES($1,$2,$3,$4,$4,0,21)",
        UUID(fixture.copy_target.candidate_id),
        uuid4(),
        row_count,
        "a" * 64,
    )


async def _validate(fixture, **changes):
    return await validate_network_membership_candidate(
        fixture.connection,
        changes.get("copy_target", fixture.copy_target),
        changes.get("address_source", fixture.address_source),
        control_schema=fixture.control_schema,
    )


async def _candidate_state(fixture):
    return await fixture.connection.fetchrow(
        f'SELECT state,validation_json,index_ready,created_at FROM "{fixture.control_schema}".network_membership_candidate'
    )


async def _assert_unvalidated(fixture):
    candidate_state = await _candidate_state(fixture)
    assert tuple(candidate_state)[:3] == ("sealed", None, False)
    for index_name in ("network_membership_network_idx", "network_membership_projection_idx"):
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", fixture.copy_target.schema_name + "." + index_name
            )
            is None
        )


@pytest.mark.asyncio
async def test_valid_report_and_exact_replay(validation_db):
    fixture = validation_db
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        report = await _validate(fixture)
        await asyncio.sleep(0)
        assert len(logged_queries) == 9
    original_state = await _candidate_state(fixture)
    assert original_state["state"] == "validated" and original_state["index_ready"] is False
    assert json.loads(original_state["validation_json"]) == report
    assert report["membership_rows"] == report["raw_rows"] == report["batch_rows"] == 4
    assert report["batch_count"] == 2 and report["distinct_memberships"] == 3 and report["projected_locations"] == 2
    assert report["unknown_network_rows"] == 0 and report["accounting_errors"] == report["diagnostics"] == []
    assert report["source_generations"] == {"unified_address": fixture.address_source.generation_id}
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        assert await _validate(fixture) == report
        await asyncio.sleep(0)
        assert len(logged_queries) == 3
    assert await _candidate_state(fixture) == original_state


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_id,unknown_rows", [(88, 1), (42, 2)])
async def test_unknown_identity_rejects_alias_fallback(validation_db, unknown_id, unknown_rows):
    fixture = validation_db
    await fixture.connection.execute(
        f'DELETE FROM "{fixture.control_schema}".network_registry_identity WHERE network_id=$1', unknown_id
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_alias '
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('synthetic','source','legacy_checksum',$1,'medical',7,'reviewed-fixture')",
        str(unknown_id),
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError) as rejected:
            await _validate(fixture)
        assert rejected.value.report["unknown_network_rows"] == unknown_rows
        assert rejected.value.report["valid"] is False
        await _assert_unvalidated(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_binding", "wrong_provider", "wrong_site", "wrong_entity"])
async def test_exact_binding_failure_rolls_back(validation_db, damage):
    fixture = validation_db
    binding_table = f'"{fixture.copy_target.schema_name}".provider_location_binding'
    if damage == "missing_binding":
        await fixture.connection.execute(f"DELETE FROM {binding_table} WHERE provider_id='provider-b'")
    else:
        assignments_by_damage = {
            "wrong_provider": "provider_id='other-provider'",
            "wrong_site": "location_id='00000000-0000-0000-0000-000000000001'::uuid",
            "wrong_entity": "entity_id='other-provider'",
        }
        await fixture.connection.execute(
            f"UPDATE {binding_table} SET {assignments_by_damage[damage]} WHERE provider_id='provider-b'"
        )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="exact provider/site"):
            await _validate(fixture)
        await _assert_unvalidated(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["ledger_missing", "raw_missing", "expected_more"])
async def test_incomplete_accounting_is_rejected(validation_db, damage):
    fixture = validation_db
    if damage == "ledger_missing":
        await fixture.connection.execute(f'DELETE FROM "{fixture.control_schema}".network_membership_batch')
    elif damage == "raw_missing":
        await fixture.connection.execute(
            f'DELETE FROM "{fixture.copy_target.schema_name}".network_membership WHERE network_id=88'
        )
    else:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=5'
        )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError) as rejected:
            await _validate(fixture)
        assert rejected.value.report["accounting_errors"] and len(rejected.value.report["diagnostics"]) <= 20
        await _assert_unvalidated(fixture)


@pytest.mark.asyncio
async def test_scope_pins_and_report_tampering(validation_db):
    fixture = validation_db
    with pytest.raises(NetworkMembershipValidationError, match="caller-owned"):
        await _validate(fixture)
    async with fixture.connection.transaction():
        for field in ("dataset_id", "schema_id", "producer_id"):
            with pytest.raises(NetworkMembershipValidationError, match="ownership"):
                await _validate(fixture, copy_target=replace(fixture.copy_target, **{field: str(uuid4())}))
        with pytest.raises(NetworkMembershipValidationError, match="generation mismatch"):
            await _validate(fixture, address_source=replace(fixture.address_source, generation_id="stale"))
        report = await _validate(fixture)
        with pytest.raises(NetworkMembershipValidationError, match="immutable pinned scope"):
            await _validate(
                fixture, address_source=replace(fixture.address_source, table_name="different_retained_table")
            )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET schema_revision=schema_revision+1'
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="immutable pinned scope"):
            await _validate(fixture)
    assert json.loads((await _candidate_state(fixture))["validation_json"]) == report


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "ready", "published", "rejected"])
async def test_closed_states_cannot_validate(validation_db, state):
    fixture = validation_db
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state=$1,validation_json='{{}}',index_ready=true",
        state,
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="sealed or validated"):
            await _validate(fixture)


@pytest.mark.asyncio
async def test_empty_and_outer_rollback(validation_db):
    fixture = validation_db
    await fixture.connection.execute(f'DELETE FROM "{fixture.copy_target.schema_name}".network_membership')
    await fixture.connection.execute(f'DELETE FROM "{fixture.control_schema}".network_membership_batch')
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=0,accepted_rows=0'
    )
    outer_transaction = fixture.connection.transaction()
    await outer_transaction.start()
    try:
        report = await _validate(fixture)
        assert report["valid"] is True and report["membership_rows"] == report["batch_rows"] == 0
    finally:
        await outer_transaction.rollback()
    await _assert_unvalidated(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "assignment,error_type",
    [
        ("NULL", asyncpg.NotNullViolationError),
        ("0", asyncpg.CheckViolationError),
        ("2147483648", asyncpg.NumericValueOutOfRangeError),
    ],
)
async def test_native_int4_constraints_remain(validation_db, assignment, error_type):
    fixture = validation_db
    with pytest.raises(error_type):
        await fixture.connection.execute(
            f'INSERT INTO "{fixture.copy_target.schema_name}".network_membership '
            f"SELECT {assignment},provider_system,provider_id,location_id,'invalid-fixture' "
            f'FROM "{fixture.copy_target.schema_name}".network_membership LIMIT 1'
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,expression,error_type",
    [
        ("provider_system", "NULL", asyncpg.NotNullViolationError),
        ("provider_id", "NULL", asyncpg.NotNullViolationError),
        ("evidence_id", "NULL", asyncpg.NotNullViolationError),
        ("location_id", "NULL", asyncpg.NotNullViolationError),
        ("location_id", "'malformed-site'", asyncpg.InvalidTextRepresentationError),
        ("location_id", "'00000000-0000-0000-0000-000000000000'", asyncpg.CheckViolationError),
        ("provider_system", "'unsupported-system'", asyncpg.CheckViolationError),
        ("provider_id", "' '", asyncpg.CheckViolationError),
        ("evidence_id", "' '", asyncpg.CheckViolationError),
        ("provider_id", "repeat('x',1025)", asyncpg.CheckViolationError),
        ("evidence_id", "repeat('x',1025)", asyncpg.CheckViolationError),
    ],
)
async def test_native_relationship_constraints_remain(validation_db, field, expression, error_type):
    fixture = validation_db
    selected_columns = ",".join(
        expression if column_name == field else column_name
        for column_name in ("network_id", "provider_system", "provider_id", "location_id", "evidence_id")
    )
    with pytest.raises(error_type):
        await fixture.connection.execute(
            f'INSERT INTO "{fixture.copy_target.schema_name}".network_membership SELECT {selected_columns} '
            f'FROM "{fixture.copy_target.schema_name}".network_membership LIMIT 1'
        )


@pytest.mark.asyncio
async def test_replay_rejects_new_source_winner(validation_db):
    fixture = validation_db
    async with fixture.connection.transaction():
        original_report = await _validate(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET source_generations=jsonb_build_object('unified_address','new-source')"
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="generation mismatch"):
            await _validate(fixture)
        with pytest.raises(NetworkMembershipValidationError, match="immutable pinned scope"):
            await _validate(fixture, address_source=replace(fixture.address_source, generation_id="new-source"))
    assert json.loads((await _candidate_state(fixture))["validation_json"]) == original_report


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra_source",
    [
        {"other": 2026},
        {"other": True},
        {"other": None},
        {"other": []},
        {"other": {"edition": "one"}},
        {"": "edition"},
        {" ": "edition"},
        {"bad\nkey": "edition"},
        {"x" * 129: "edition"},
        {"other": ""},
        {"other": " "},
        {"other": "bad\tvalue"},
        {"other": "x" * 257},
        {str(index): "edition" for index in range(50)},
    ],
)
async def test_source_map_rejects_untyped_metadata(validation_db, extra_source):
    fixture = validation_db
    generations_by_source = {"unified_address": fixture.address_source.generation_id} | extra_source
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET source_generations=$1::jsonb',
        json.dumps(generations_by_source),
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="source generations"):
            await _validate(fixture)
        await _assert_unvalidated(fixture)


@pytest.mark.asyncio
async def test_complete_source_map_preserves_strings(validation_db):
    fixture = validation_db
    generations_by_source = {str(index): "x" * 256 for index in range(48)}
    generations_by_source |= {"unified_address": fixture.address_source.generation_id, "k" * 128: " édition / 2026 "}
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET source_generations=$1::jsonb',
        json.dumps(generations_by_source),
    )
    async with fixture.connection.transaction():
        assert (await _validate(fixture))["source_generations"] == generations_by_source


@pytest.mark.asyncio
async def test_set_queries_remain_bounded(validation_db):
    fixture = validation_db
    for batch_index in range(2):
        await fixture.connection.copy_records_to_table(
            "network_membership",
            schema_name=fixture.copy_target.schema_name,
            records=[
                (42, "npi", "provider-a", fixture.location_ids[0], f"extra-{batch_index}-{index}")
                for index in range(5000)
            ],
        )
        await _batch(fixture, 5000)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=10004,accepted_rows=10004'
    )
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        report = await _validate(fixture)
        await asyncio.sleep(0)
        assert len(logged_queries) == 9 and report["raw_rows"] == 10004
        await fixture.connection.execute("SET LOCAL enable_seqscan=off")
        query_plan = await fixture.connection.fetch(
            f'EXPLAIN SELECT membership.network_id FROM "{fixture.copy_target.schema_name}".network_membership membership '
            f'LEFT JOIN "{fixture.control_schema}".network_registry_identity identity ON identity.network_id=membership.network_id '
            "WHERE identity.network_id IS NULL"
        )
    plan_text = "\n".join(entry[0] for entry in query_plan)
    assert any(
        index_name in plan_text
        for index_name in ("network_membership_network_idx", "network_membership_projection_idx")
    )
    assert "network_registry_identity_pkey" in plan_text


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["schema_revision", "expected_head"])
async def test_replay_metadata_types_remain_exact(validation_db, field):
    fixture = validation_db
    async with fixture.connection.transaction():
        report = await _validate(fixture)
    report[field] = bool(report[field])
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$1::jsonb',
        json.dumps(report),
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkMembershipValidationError, match="immutable pinned scope"):
            await _validate(fixture)
