# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact retained site adoption against real serving publication and closure."""

import asyncio
import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MAX_INPUT_BYTES, MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions
from tests.test_network_custom_address_source_postgres import _draft, custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _create
from tests.test_registry_candidate_composition_postgres import _roles

pytestmark = pytest.mark.asyncio


async def _source_addresses(fixture, count, provider_system):
    """Create exact synthetic offices in the fixture's full model address table."""
    connection = fixture.connection
    provider_id = "1000000004" if provider_system == "npi" else str(uuid4())
    database_name = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    source_table = f'"{fixture.source_schema}".entity_address_unified'
    await connection.execute(
        f"UPDATE {source_table} SET entity_type=$1,entity_id=$2,npi=$3",
        provider_system,
        provider_id,
        1000000004 if provider_system == "npi" else None,
    )
    columns = await connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        source_table,
    )
    expression_by_column = {
        "location_key": "lpad(to_hex(series),64,'0')",
        "second_line": "'Suite '||series::text",
        "checksum": "42+series",
    }
    await connection.execute(
        f"INSERT INTO {source_table} SELECT "
        + ",".join(expression_by_column.get(column["attname"], f'original."{column["attname"]}"') for column in columns)
        + f" FROM {source_table} original CROSS JOIN generate_series(1,$1::int) series WHERE original.location_key=$2",
        count - 1,
        "a" * 64,
    )
    return provider_id, source_table


async def _published(fixture, count, provider_system, copy_targets):
    """Publish full model addresses through the actual index and closure gates."""
    connection = fixture.connection
    provider_id, source_table = await _source_addresses(fixture, count, provider_system)
    network = await _draft(fixture, _create("network"), _actor())
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    copy_targets.append(copy_target)
    async with connection.transaction():
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={"unified_address": fixture.base.generation_id, "fhir": "retained-edition-a"},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=count,
            control_schema=fixture.control_schema,
        )
        await connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))''')
        await connection.execute(
            f'''INSERT INTO "{copy_target.schema_name}".provider_location_binding
              SELECT $1,$2,md5(location_key)::uuid,location_key,entity_type,entity_id FROM {source_table}''',
            provider_system,
            provider_id,
        )
        bindings = await connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".provider_location_binding')
        input_bytes = json.dumps(
            [
                {
                    "network_id": network["record_id"],
                    "provider_system": binding["provider_system"],
                    "provider_id": binding["provider_id"],
                    "location_id": str(binding["location_id"]),
                    "evidence_id": "e" * 64,
                }
                for binding in bindings
            ]
        ).encode()
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=fixture.control_schema)
    await prepare_and_publish_network_candidate(
        connection, copy_target, fixture.base, **_roles(fixture), control_schema=fixture.control_schema
    )
    async with connection.transaction(readonly=True):
        return await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)


@pytest.fixture
async def retained_db(custom_db, request):
    """Keep every created candidate and role scoped to this native test fixture."""
    count, system = getattr(request, "param", (2, "npi"))
    copy_targets = []
    try:
        source = await _published(custom_db, count, system, copy_targets)
        rows = await custom_db.connection.fetch(
            f'''SELECT binding.provider_system,binding.provider_id,binding.location_id::text,binding.location_key,
              encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
              FROM "{source.schema_name}".provider_location_binding binding
              JOIN "{source.schema_name}".entity_address_unified address USING(location_key)
              ORDER BY binding.location_key'''
        )
        yield SimpleNamespace(**vars(custom_db), source=source, rows=[dict(row) for row in rows])
    finally:
        for copy_target in copy_targets:
            await custom_db.connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
            assert await custom_db.connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None


async def _resolve(fixture, rows=None, *, source=None, isolation="repeatable_read"):
    """Use an explicitly pinned readonly caller transaction for each adoption."""
    async with fixture.connection.transaction(isolation=isolation, readonly=True):
        return await resolve_retained_site_adoptions(
            fixture.connection,
            source or fixture.source,
            json.dumps(fixture.rows[:1] if rows is None else rows).encode(),
            control_schema=fixture.control_schema,
        )


@pytest.mark.parametrize("retained_db", [(2, "npi"), (2, "provider_directory")], indirect=True)
async def test_selected_office_only_and_deterministic_immutable_receipt(retained_db):
    fixture = retained_db
    receipt = await _resolve(fixture)
    assert receipt == await _resolve(fixture, isolation="serializable")
    assert len(receipt.records) == 1 and receipt.records[0].location_id == fixture.rows[0]["location_id"]
    assert fixture.rows[1]["location_id"] not in json.dumps(receipt.as_dict())
    assert receipt.address_table_oid == fixture.source.address_table_oid
    assert receipt.schema_oid > 0 and receipt.binding_table_oid > 0 and receipt.owner_role_oid > 0
    assert dict(receipt.source_generations) == fixture.source.source_generations
    assert receipt.records[0].entity_type == fixture.rows[0]["provider_system"]
    assert receipt.records[0].entity_id == fixture.rows[0]["provider_id"]
    with pytest.raises(FrozenInstanceError):
        receipt.records[0].location_key = "b" * 64
    envelope = receipt.as_dict()
    envelope["source_generations"].clear()
    envelope["records"][0]["location_key"] = "b" * 64
    assert receipt == await _resolve(fixture)
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema())"
    )


@pytest.mark.parametrize("damage", ["namespace", "site", "key", "hash", "duplicate", "identity"])
async def test_selection_rejects_whole_batch(retained_db, damage):
    fixture = retained_db
    row = fixture.rows[0].copy()
    changes_by_damage = {
        "namespace": {"provider_system": "provider_directory"},
        "site": {"location_id": str(uuid4())},
        "key": {"location_key": fixture.rows[1]["location_key"]},
        "hash": {"address_row_sha256": "0" * 64},
        "identity": {"provider_id": "1000000005"},
    }
    row.update(changes_by_damage.get(damage, {}))
    with pytest.raises(RetainedSiteAdoptionError):
        await _resolve(fixture, [fixture.rows[0], row] if damage == "duplicate" else [fixture.rows[1], row])


@pytest.mark.parametrize("field", ["generation_id", "address_table_oid", "source_generations", "manifest_sha256"])
async def test_supplied_source_must_equal_verified_manifest(retained_db, field):
    fixture = retained_db
    changes_by_field = {
        "generation_id": fixture.source.generation_id + 1,
        "address_table_oid": fixture.source.address_table_oid + 1,
        "source_generations": {"fhir": "changed-retained-edition"},
        "manifest_sha256": "0" * 64,
    }
    with pytest.raises(RetainedSiteAdoptionError):
        await _resolve(fixture, source=replace(fixture.source, **{field: changes_by_field[field]}))


@pytest.mark.parametrize("damage", ["acl", "ineligible", "source_fingerprint", "row_fingerprint", "binding_oid"])
async def test_native_tampering_fails_closed_and_caller_rollback_restores(retained_db, damage):
    fixture = retained_db
    connection = fixture.connection
    namespace = '"' + fixture.source.schema_name + '"'
    transaction = connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        statement_by_damage = {
            "acl": f'GRANT UPDATE ON {namespace}.provider_location_binding TO "{fixture.roles["reader"]}"',
            "ineligible": f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false',
            "source_fingerprint": f'''UPDATE "{fixture.control_schema}".network_membership_candidate
              SET source_generations='{{"fhir":"changed"}}'::jsonb''',
            "row_fingerprint": f"UPDATE {namespace}.entity_address_unified SET second_line='Suite changed'",
            "binding_oid": f"ALTER TABLE {namespace}.provider_location_binding RENAME TO old_binding",
        }
        await connection.execute(statement_by_damage[damage])
        with pytest.raises(RetainedSiteAdoptionError):
            await resolve_retained_site_adoptions(
                connection, fixture.source, json.dumps(fixture.rows[:1]).encode(), control_schema=fixture.control_schema
            )
    finally:
        await transaction.rollback()
    assert len((await _resolve(fixture)).records) == 1


@pytest.mark.parametrize("payload", [b"[]", b"{}", b"null", b"[NaN]", b"[{}]", b"[", b" " * (MAX_INPUT_BYTES + 1)])
async def test_strict_input_is_rejected_before_database(retained_db, payload):
    with pytest.raises(RetainedSiteAdoptionError):
        await resolve_retained_site_adoptions(None, retained_db.source, payload)


async def test_wire_fields_and_native_identity_bounds(retained_db):
    fixture = retained_db
    changes = [
        {"extra": "field"},
        {"provider_system": "manual"},
        {"provider_id": True},
        {"location_id": str(UUID(int=0))},
        {"location_key": "A" * 64},
        {"address_row_sha256": "A" * 64},
    ]
    for change in changes:
        with pytest.raises(RetainedSiteAdoptionError):
            await _resolve(fixture, [{**fixture.rows[0], **change}])
    assert await _resolve(
        fixture, [{**fixture.rows[0], "location_id": fixture.rows[0]["location_id"].upper()}]
    ) == await _resolve(fixture)
    duplicate_field = json.dumps(fixture.rows[:1]).replace(
        '"provider_system":', '"provider_system":"npi","provider_system":'
    )
    with pytest.raises(RetainedSiteAdoptionError):
        await resolve_retained_site_adoptions(None, fixture.source, duplicate_field.encode())
    with pytest.raises(RetainedSiteAdoptionError):
        await _resolve(fixture, fixture.rows[:1] * 5001)


async def test_transaction_contract_and_readonly_query_count(retained_db):
    fixture = retained_db
    with pytest.raises(RetainedSiteAdoptionError):
        await _resolve(fixture, isolation="read_committed")
    with pytest.raises(RetainedSiteAdoptionError):
        await resolve_retained_site_adoptions(fixture.connection, fixture.source, json.dumps(fixture.rows[:1]).encode())
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            await resolve_retained_site_adoptions(
                fixture.connection,
                fixture.source,
                json.dumps(fixture.rows[:1]).encode(),
                control_schema=fixture.control_schema,
            )
            await asyncio.sleep(0)
            assert len(queries) == 3
            assert all(query.query.lstrip().split()[0] in ("SELECT", "WITH") for query in queries)
    finally:
        fixture.connection.remove_query_logger(queries.append)


@pytest.mark.parametrize("retained_db", [(5000, "npi")], indirect=True)
async def test_maximum_batch_has_the_same_three_native_queries(retained_db):
    fixture = retained_db
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            receipt = await resolve_retained_site_adoptions(
                fixture.connection,
                fixture.source,
                json.dumps(fixture.rows).encode(),
                control_schema=fixture.control_schema,
            )
            await asyncio.sleep(0)
            assert len(receipt.records) == 5000 and len(queries) == 3
    finally:
        fixture.connection.remove_query_logger(queries.append)
