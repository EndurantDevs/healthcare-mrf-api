# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable named generations through genuine preparation and publication."""

import json
from dataclasses import FrozenInstanceError
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from process.network_address_projection import _identifier
from process.network_serving_read import (
    NetworkServingReadUnavailable,
    parse_canonical_network_ids,
    resolve_network_serving_manifest,
)
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _reader,
    _scoped_rows,
    _seal,
)
from tests.test_network_compact_publication_postgres import (
    compact_publication_db as compact_publication_db,
)
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_provider_routes_postgres import provider_app as provider_app
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


@pytest.fixture
async def read_component_db(compact_publication_db):
    fixture = compact_publication_db
    fixture.copy_target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, fixture.copy_target)
    await _seal(fixture, fixture.copy_target)
    fixture.read_manifest = await _publish(fixture, fixture.copy_target)
    yield fixture


async def _resolve(fixture, generation_id=None, connection=None, *, api_read=False):
    connection = connection or fixture.connection
    async with connection.transaction():
        return await resolve_network_serving_manifest(
            connection, generation_id=generation_id, control_schema=fixture.control_schema, api_read=api_read
        )


async def _control_rows(fixture):
    namespace = _identifier(fixture.control_schema)
    return tuple(
        [
            await fixture.connection.fetch(f"SELECT to_jsonb(row) FROM {namespace}.{table} row ORDER BY 1")
            for table in ("network_membership_candidate", "network_serving_manifest", "network_serving_control")
        ]
    )


async def test_retained_resolver_keeps_named_generation(read_component_db):
    from process.registry_retained_generation import resolve_registry_retained_generation

    fixture = read_component_db
    before = await _control_rows(fixture)
    locator_by_field = {"source_generations": {"retained_network_source": fixture.read_manifest["manifest_sha256"]}}
    async with _reader(fixture) as (_, connection):
        manifest, candidate = await resolve_registry_retained_generation(
            connection, locator_by_field, control_schema=fixture.control_schema
        )
        assert manifest == await resolve_network_serving_manifest(
            connection, generation_id=manifest.generation_id, control_schema=fixture.control_schema
        )
        assert manifest.schema_name == candidate["schema_name"] == fixture.copy_target.schema_name
        assert manifest.address_relation == manifest.compact_layout.base
        assert all(pin.schema_name and pin.table_name for pin in manifest.candidate_relation_pins)
    assert await _control_rows(fixture) == before


async def test_current_and_retained_pin_after_concurrent_head_advance(read_component_db):
    fixture = read_component_db
    first = await _resolve(fixture)
    successor = await _candidate(fixture, (88, 42), first.generation_id)
    await _prepare(fixture, successor)
    await _seal(fixture, successor)
    async with _reader(fixture) as (connection, native):
        pin = await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        before = await _scoped_rows(connection, pin, (42,))
        second = await _publish(fixture, successor)
        assert await resolve_network_serving_manifest(native, control_schema=fixture.control_schema) == first
        assert await _scoped_rows(connection, pin, (42,)) == before
    assert (await _resolve(fixture)).generation_id == second["generation_id"]
    assert await _resolve(fixture, first.generation_id) == first
    assert first.candidate_id == fixture.copy_target.candidate_id
    assert first.source_generations == fixture.read_manifest["source_generations"]
    assert first.approved_custom_revision == fixture.read_manifest["approved_custom_revision"]
    assert first.manifest_sha256 == fixture.read_manifest["manifest_sha256"]
    with pytest.raises(FrozenInstanceError):
        first.generation_id = second["generation_id"]


async def test_resolver_uses_bounded_metadata_reads_with_no_state_or_sequence_writes(read_component_db):
    fixture = read_component_db
    before = await _control_rows(fixture)
    sequence = f'"{fixture.control_schema}".network_serving_manifest_generation_id_seq'
    sequence_before = await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")

    class ReadOnlyConnection:
        def __init__(self):
            self.requests = []

        def is_in_transaction(self):
            return fixture.connection.is_in_transaction()

        async def fetchrow(self, statement, *parameters):
            assert statement.lstrip().startswith("SELECT jsonb_build_object(")
            assert "FOR UPDATE" not in statement and "FOR SHARE" not in statement
            assert f'FROM "{fixture.control_schema}".network_serving_manifest manifest' in statement
            assert "coalesce(octet_length(candidate.validation_json::text),0)<=1048576" in statement
            assert "octet_length(candidate.source_generations::text)<=1048576" in statement
            assert "octet_length(manifest.source_generations::text)<=1048576" in statement
            assert f'"{fixture.copy_target.schema_name}".' not in statement
            self.requests.append(parameters)
            return await fixture.connection.fetchrow(statement, *parameters)

    connection = ReadOnlyConnection()
    async with fixture.connection.transaction(readonly=True):
        first = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        second = await resolve_network_serving_manifest(
            connection, generation_id=first.generation_id, control_schema=fixture.control_schema
        )
    assert first == second and connection.requests == [(None, False), (first.generation_id, False)]
    assert await _control_rows(fixture) == before
    assert await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == sequence_before


async def test_readonly_reader_resolves_and_native_permissions_deny_writes(read_component_db):
    fixture = read_component_db
    async with _reader(fixture) as (_, connection):
        pin = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        assert pin.generation_id == fixture.read_manifest["generation_id"]
        assert await connection.fetchval(f'SELECT count(*) FROM "{pin.schema_name}".network_membership') == 2
    async with fixture.reader_engine.connect() as connection:
        with pytest.raises(ProgrammingError) as caught:
            await connection.execute(text(f'UPDATE "{pin.schema_name}".network_membership SET network_id=network_id'))
        assert caught.value.orig.sqlstate == "42501"


@pytest.mark.parametrize("damage", ["source", "custom", "schema_revision", "retired", "candidate_state"])
@pytest.mark.parametrize("api_read", [False, True])
async def test_changed_manifest_scope_or_eligibility_is_unavailable(read_component_db, damage, api_read):
    fixture = read_component_db
    changes_by_damage = {
        "source": "source_generations='{}'::jsonb",
        "custom": "approved_custom_revision=1",
        "schema_revision": "schema_revision=2",
        "retired": "eligible=false",
    }
    table = "network_membership_candidate" if damage == "candidate_state" else "network_serving_manifest"
    change = "state='rejected'" if damage == "candidate_state" else changes_by_damage[damage]
    await fixture.connection.execute(f'UPDATE "{fixture.control_schema}".{table} SET {change}')
    for generation_id in (None, fixture.read_manifest["generation_id"]):
        with pytest.raises(NetworkServingReadUnavailable):
            await _resolve(fixture, generation_id, api_read=api_read)


@pytest.mark.parametrize("field", ["validation_json", "candidate_sources", "manifest_sources"])
@pytest.mark.parametrize("api_read", [False, True])
async def test_oversized_metadata_refuses_before_decode(read_component_db, field, api_read):
    fixture = read_component_db
    table = "network_serving_manifest" if field == "manifest_sources" else "network_membership_candidate"
    column = "validation_json" if field == "validation_json" else "source_generations"
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".{table} SET {column}=jsonb_build_object($1::text,repeat($2::text,1048577))',
        "padding",
        "x",
    )
    for generation_id in (None, fixture.read_manifest["generation_id"]):
        with pytest.raises(NetworkServingReadUnavailable, match="generation is unavailable"):
            await _resolve(fixture, generation_id, api_read=api_read)


async def test_invalid_candidate_content_cannot_replace_published_head(read_component_db):
    fixture = read_component_db
    head = await _resolve(fixture)
    successor = await _candidate(fixture, (42, 2147483647), head.generation_id)
    with pytest.raises(ValueError):
        await _prepare(fixture, successor)
    assert await _resolve(fixture) == head
    state = await fixture.connection.fetchval(
        f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(successor.candidate_id),
    )
    assert state == "sealed"


@pytest.mark.parametrize("damage", ["missing_readiness", "scope", "readiness"])
async def test_logical_readiness_damage_cannot_replace_published_head(read_component_db, damage):
    fixture = read_component_db
    head = await _resolve(fixture)
    successor = await _candidate(fixture, (88, 42), head.generation_id)
    await _prepare(fixture, successor)
    await _seal(fixture, successor)
    candidate = UUID(successor.candidate_id)
    report = json.loads(
        await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            candidate,
        )
    )
    if damage == "missing_readiness":
        report.pop("candidate_readiness")
    elif damage == "scope":
        report["serving_readiness"]["scope"]["producer_id"] = str(uuid4())
    else:
        report["serving_readiness"]["ready"] = False
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$1::jsonb '
        "WHERE candidate_id=$2",
        json.dumps(report),
        candidate,
    )
    with pytest.raises(ValueError):
        await _publish(fixture, successor)
    assert await _resolve(fixture) == head
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 1
    )


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


async def test_generation_type_transaction_and_namespace_are_required(read_component_db):
    fixture = read_component_db
    with pytest.raises(NetworkServingReadUnavailable, match="active transaction"):
        await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    for generation_id in (True, False, 0, -1, 1.0, "1", 9223372036854775808):
        with pytest.raises(NetworkServingReadUnavailable, match="generation is invalid"):
            await _resolve(fixture, generation_id)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingReadUnavailable):
            await resolve_network_serving_manifest(fixture.connection, control_schema='invalid";DROP SCHEMA public;')


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


async def test_positive_legacy_42_does_not_expand_canonical_selector(read_component_db):
    fixture = read_component_db
    async with _reader(fixture) as (connection, native):
        pin = await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        canonical = await _scoped_rows(connection, pin, parse_canonical_network_ids("42"))
        relation = pin.compact_layout.base
        legacy = await native.fetch(
            f'SELECT location_key FROM "{relation.schema_name}"."{relation.table_name}" '
            "WHERE plans_network_array && $1::integer[] ORDER BY location_key",
            [42],
        )
    assert len(canonical) == 1 and len(legacy) == 2


async def test_qualified_current_and_retained_reader_keeps_named_source(provider_app):
    fixture = provider_app.fixture
    current = await _resolve(fixture)
    retained = await _resolve(fixture, provider_app.source_manifest.generation_id)
    assert retained == provider_app.source_manifest and current.schema_name == provider_app.current_schema
    assert "registry_source_retention" in current.source_generations
    source = _identifier(current.schema_name) + ".network_membership_source"
    async with provider_app.engine.connect() as connection:
        assert await connection.scalar(text(f"SELECT count(*) FROM {source}")) > 0
        with pytest.raises(ProgrammingError) as caught:
            await connection.execute(text(f"UPDATE {source} SET evidence_id=evidence_id"))
        assert caught.value.orig.sqlstate == "42501"
