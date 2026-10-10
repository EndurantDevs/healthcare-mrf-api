# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable serving pins through protected candidate writer closure."""

import json
from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest

from process.network_address_projection import _identifier
from process.network_serving_read import (
    NetworkServingReadUnavailable,
    parse_canonical_network_ids,
    resolve_network_serving_manifest,
)
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_candidate_indexes_postgres import indexed_db
from tests.test_network_membership_publication_postgres import (
    _candidate_writer_roles as _component_owner,
)
from tests.test_network_membership_publication_postgres import (
    _publish,
    _second_candidate,
    _snapshot,
    _write_report,
    publication_db,
)
from tests.test_network_membership_serving_indexes_postgres import _prepare, serving_indexes_db
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def read_component_db(publication_db):
    """Publish an actually frozen candidate through its native publisher role."""
    async with _component_owner(publication_db) as fixture:
        async with fixture.connection.transaction():
            fixture.read_manifest = await _publish(fixture)
        yield fixture


async def _resolve(fixture, generation_id=None, connection=None):
    connection = connection or fixture.connection
    async with connection.transaction():
        return await resolve_network_serving_manifest(
            connection, generation_id=generation_id, control_schema=fixture.control_schema
        )


@pytest.mark.asyncio
async def test_current_and_retained_pin_after_concurrent_head_advance(read_component_db):
    fixture = read_component_db
    first = await _resolve(fixture)
    second_fixture = await _second_candidate(fixture, expected_head=first.generation_id)
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        assert await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema) == first
        async with fixture.connection.transaction():
            second_manifest = await _publish(second_fixture)
        assert await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema) == first
    assert (await _resolve(fixture)).generation_id == second_manifest["generation_id"]
    assert await _resolve(fixture, first.generation_id) == first
    assert first.candidate_id == fixture.copy_target.candidate_id
    assert first.source_generations == fixture.read_manifest["source_generations"]
    assert first.approved_custom_revision == fixture.read_manifest["approved_custom_revision"]
    assert first.manifest_sha256 == fixture.read_manifest["manifest_sha256"]
    with pytest.raises(FrozenInstanceError):
        first.generation_id = second_manifest["generation_id"]


@pytest.mark.asyncio
async def test_resolver_is_one_select_with_no_state_or_sequence_writes(read_component_db):
    fixture = read_component_db
    before = await _snapshot(fixture)
    sequence = f'"{fixture.control_schema}".network_serving_manifest_generation_id_seq'
    sequence_before = await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")

    class ReadOnlyConnection:
        statements = 0

        def is_in_transaction(self):
            return fixture.connection.is_in_transaction()

        async def fetchrow(self, statement, *parameters):
            self.statements += 1
            assert statement.lstrip().startswith("SELECT")
            assert "FOR UPDATE" not in statement and "FOR SHARE" not in statement
            return await fixture.connection.fetchrow(statement, *parameters)

    connection = ReadOnlyConnection()
    async with fixture.connection.transaction(readonly=True):
        first = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        second = await resolve_network_serving_manifest(
            connection, generation_id=first.generation_id, control_schema=fixture.control_schema
        )
    assert first == second and connection.statements == 2
    assert await _snapshot(fixture) == before
    assert await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == sequence_before


async def test_readonly_reader_needs_no_owner_set_privilege(read_component_db):
    fixture = read_component_db
    connection = fixture.observer
    await connection.execute(f'SET ROLE "{fixture.writer_roles["reader"]}"')
    try:
        assert not await connection.fetchval(
            "SELECT pg_has_role(current_user,$1::name,'SET')", fixture.writer_roles["owner"]
        )
        async with connection.transaction(readonly=True):
            pin = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        assert pin.generation_id == fixture.read_manifest["generation_id"]
    finally:
        await connection.execute("RESET ROLE")


@pytest.mark.parametrize(
    "damage",
    ["schema_public", "table_write", "column_write", "grant_option", "owner_member", "createrole", "default_acl"],
)
async def test_current_and_retained_native_acl_drift_is_unavailable(read_component_db, damage):
    fixture = read_component_db
    namespace = _identifier(fixture.copy_target.schema_name)
    command_by_damage = {
        "schema_public": f"GRANT USAGE ON SCHEMA {namespace} TO PUBLIC",
        "table_write": f'GRANT UPDATE ON {namespace}.network_membership TO "{fixture.writer_roles["loader"]}"',
        "column_write": f"GRANT UPDATE(network_id) ON {namespace}.network_membership TO PUBLIC",
        "grant_option": f'GRANT SELECT ON {namespace}.network_membership TO "{fixture.writer_roles["reader"]}" WITH GRANT OPTION',
        "owner_member": f'GRANT "{fixture.writer_roles["owner"]}" TO "{fixture.writer_roles["reader"]}"',
        "createrole": f'ALTER ROLE "{fixture.writer_roles["reader"]}" CREATEROLE',
        "default_acl": f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.writer_roles["owner"]}" '
        f"IN SCHEMA {namespace} GRANT INSERT ON TABLES TO PUBLIC",
    }
    await fixture.connection.execute(command_by_damage[damage])
    for generation_id in (None, fixture.read_manifest["generation_id"]):
        with pytest.raises(NetworkServingReadUnavailable):
            await _resolve(fixture, generation_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["digest", "source", "custom", "schema_revision", "retired", "candidate_state"])
async def test_changed_manifest_scope_digest_or_eligibility_is_unavailable(read_component_db, damage):
    fixture = read_component_db
    changes_by_name = {
        "digest": "manifest_sha256='" + "0" * 64 + "'",
        "source": "source_generations='{}'::jsonb",
        "custom": "approved_custom_revision=1",
        "schema_revision": "schema_revision=2",
        "retired": "eligible=false",
    }
    if damage == "candidate_state":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='rejected'"
        )
    else:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_manifest SET {changes_by_name[damage]}'
        )
    for generation_id in (None, fixture.read_manifest["generation_id"]):
        with pytest.raises(NetworkServingReadUnavailable):
            await _resolve(fixture, generation_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["missing", "scope", "schema_oid", "address_oid", "bool_oid", "readiness", "owner_login"]
)
async def test_closure_and_readiness_damage_is_unavailable(read_component_db, damage):
    fixture = read_component_db
    report = json.loads(
        await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate'
        )
    )
    match damage:
        case "owner_login":
            await fixture.connection.execute(f"ALTER ROLE {_identifier(fixture.read_owner_role)} LOGIN")
        case "missing":
            report.pop("writer_closure")
        case "scope":
            report["writer_closure"]["scope"]["producer_id"] = str(uuid4())
        case "schema_oid":
            report["writer_closure"]["schema_oid"] += 1
        case "address_oid" | "bool_oid":
            report["writer_closure"]["relation_oids"]["entity_address_unified"] = True if damage == "bool_oid" else 1
        case _:
            report["serving_readiness"]["ready"] = False
    await _write_report(fixture, report)
    with pytest.raises(NetworkServingReadUnavailable):
        await _resolve(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["address_oid", "address_owner", "index_oid", "view"])
async def test_current_physical_oid_type_and_owner_must_match(read_component_db, damage):
    fixture = read_component_db
    namespace = _identifier(fixture.copy_target.schema_name)
    if damage in {"address_oid", "view"}:
        await fixture.connection.execute(f"ALTER TABLE {namespace}.entity_address_unified RENAME TO retained_address")
        replacement = "TABLE" if damage == "address_oid" else "VIEW"
        await fixture.connection.execute(
            f"CREATE {replacement} {namespace}.entity_address_unified AS SELECT * FROM {namespace}.retained_address"
        )
    elif damage == "index_oid":
        await fixture.connection.execute(f"DROP INDEX {namespace}.canonical_network_ids_gin")
        await fixture.connection.execute(
            f"CREATE INDEX canonical_network_ids_gin ON {namespace}.entity_address_unified USING GIN(canonical_network_ids gin__int_ops)"
        )
    else:
        original_owner = await fixture.connection.fetchval("SELECT current_user")
        await fixture.connection.execute(
            f"ALTER TABLE {namespace}.entity_address_unified OWNER TO {_identifier(original_owner)}"
        )
    with pytest.raises(NetworkServingReadUnavailable):
        await _resolve(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["null", "missing"])
async def test_missing_current_never_falls_back_and_retained_remains_explicit(read_component_db, damage):
    fixture = read_component_db
    operation = "UPDATE" if damage == "null" else "DELETE FROM"
    statement = "SET generation_id=NULL" if damage == "null" else ""
    await fixture.connection.execute(f'{operation} "{fixture.control_schema}".network_serving_control {statement}')
    with pytest.raises(NetworkServingReadUnavailable):
        await _resolve(fixture)
    assert (await _resolve(fixture, fixture.read_manifest["generation_id"])).generation_id == fixture.read_manifest[
        "generation_id"
    ]
    with pytest.raises(NetworkServingReadUnavailable):
        await _resolve(fixture, 9223372036854775807)


@pytest.mark.asyncio
async def test_generation_type_transaction_and_namespace_are_required(read_component_db):
    fixture = read_component_db
    with pytest.raises(NetworkServingReadUnavailable, match="caller-owned"):
        await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    for generation_id in (True, False, 0, -1, 1.0, "1", 9223372036854775808):
        with pytest.raises(NetworkServingReadUnavailable, match="generation is invalid"):
            await _resolve(fixture, generation_id)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingReadUnavailable):
            await resolve_network_serving_manifest(fixture.connection, control_schema='invalid";DROP SCHEMA public;')


@pytest.mark.asyncio
async def test_actual_full_serving_gate_resolves_with_native_closure(serving_indexes_db):
    fixture = serving_indexes_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    async with _component_owner(fixture):
        async with fixture.connection.transaction():
            manifest = await _publish(fixture)
        pin = await _resolve(fixture)
        assert pin.generation_id == manifest["generation_id"]
        assert pin.address_table_oid == await fixture.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", fixture.copy_target.schema_name + ".entity_address_unified"
        )


@pytest.mark.parametrize(
    "value,expected",
    [
        ("42", (42,)),
        ("2147483647,1,42", (1, 42, 2147483647)),
        (",".join(str(number) for number in range(100, 0, -1)), tuple(range(1, 101))),
    ],
)
def test_canonical_selector_is_sorted_bounded_integer_namespace(value, expected):
    assert parse_canonical_network_ids(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        42,
        42.0,
        "",
        "0",
        "-1",
        "+42",
        "042",
        "42.0",
        "42,42",
        "42,",
        ",42",
        "42,,7",
        " 42",
        "42 ",
        "42, 7",
        "４２",
        "٤٢",
        "42\n",
        "2147483648",
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ",".join(str(number) for number in range(1, 102)),
        "1" * 1100,
    ],
)
def test_canonical_selector_rejects_ambiguous_or_noncanonical_values(value):
    with pytest.raises(ValueError, match="canonical_network_ids_invalid"):
        parse_canonical_network_ids(value)


@pytest.mark.asyncio
async def test_positive_legacy_42_does_not_expand_canonical_selector(read_component_db):
    fixture = read_component_db
    pin = await _resolve(fixture)
    selector = parse_canonical_network_ids("42")
    table = f"{_identifier(pin.schema_name)}.entity_address_unified"
    canonical = await fixture.connection.fetch(
        f"SELECT location_key FROM {table} WHERE canonical_network_ids && $1::integer[] ORDER BY location_key",
        selector,
    )
    legacy = await fixture.connection.fetch(
        f"SELECT location_key FROM {table} WHERE plans_network_array && $1::integer[] ORDER BY location_key",
        selector,
    )
    assert [row["location_key"] for row in canonical] == ["a" * 64]
    assert [row["location_key"] for row in legacy] == ["a" * 64, "c" * 64]
