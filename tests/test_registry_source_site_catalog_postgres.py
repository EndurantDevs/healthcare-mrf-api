# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact office catalog against real retained publication and native closure."""

import asyncio
import hashlib
import json
import os
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions
from process.registry_site_binding_store import validated_site_binding_fields
from process.registry_source_site_catalog import (
    RegistrySourceSiteCatalogError,
    RegistrySourceSiteCatalogUnavailable,
    read_registry_source_sites,
)
from tests.test_network_custom_address_source_postgres import _draft, custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _create
from tests.test_registry_candidate_composition_postgres import _roles
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


async def _other_provider(fixture, source_table, provider_system):
    columns = await fixture.connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        source_table,
    )
    provider_id = "1000000012" if provider_system == "npi" else str(uuid4())
    expressions_by_field = {
        "location_key": "'" + "d" * 64 + "'",
        "entity_id": "$1",
        "npi": "1000000012" if provider_system == "npi" else "NULL",
        "first_line": "'999 Other Street'",
        "checksum": "999",
    }
    await fixture.connection.execute(
        f"INSERT INTO {source_table} SELECT "
        + ",".join(expressions_by_field.get(column["attname"], f'address."{column["attname"]}"') for column in columns)
        + f" FROM {source_table} address WHERE address.location_key=$2",
        provider_id,
        "a" * 64,
    )
    return provider_id


async def _raw_candidate(fixture, source_table, copy_targets, *, expected_head=0):
    connection = fixture.connection
    network = await _draft(fixture, _create("network"), _actor())
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    copy_targets.append(copy_target)
    address_rows = await connection.fetch(f"SELECT location_key,entity_type,entity_id FROM {source_table}")
    input_bytes = json.dumps(
        [
            {
                "network_id": network["record_id"],
                "provider_system": address_record["entity_type"],
                "provider_id": address_record["entity_id"],
                "location_id": str(UUID(hashlib.md5(address_record["location_key"].encode()).hexdigest())),
                "evidence_id": "e" * 64,
            }
            for address_record in address_rows
        ]
    ).encode()
    async with connection.transaction():
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={"unified_address": fixture.base.generation_id},
            approved_custom_revision=0,
            expected_head=expected_head,
            expected_rows=len(address_rows),
            control_schema=fixture.control_schema,
        )
        namespace = '"' + copy_target.schema_name + '"'
        await connection.execute(f"""CREATE TABLE {namespace}.provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))""")
        await connection.execute(f"""INSERT INTO {namespace}.provider_location_binding
          SELECT entity_type,entity_id,md5(location_key)::uuid,location_key,entity_type,entity_id FROM {source_table}""")
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=fixture.control_schema)
    return copy_target


@pytest.fixture
async def catalog_db(custom_db, request):
    count, system = getattr(request, "param", (2, "npi"))
    copy_targets = []
    try:
        provider_id, source_table = await _source_addresses(custom_db, count, system)
        other_id = await _other_provider(custom_db, source_table, system)
        target = await _raw_candidate(custom_db, source_table, copy_targets)
        await prepare_and_publish_network_candidate(
            custom_db.connection, target, custom_db.base, **_roles(custom_db), control_schema=custom_db.control_schema
        )
        async with custom_db.connection.transaction(readonly=True):
            source = await resolve_network_serving_manifest(
                custom_db.connection, control_schema=custom_db.control_schema
            )
        yield SimpleNamespace(
            **vars(custom_db),
            source=source,
            provider_id=provider_id,
            provider_system=system,
            other_id=other_id,
            copy_targets=copy_targets,
        )
    finally:
        for target in copy_targets:
            await custom_db.connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')
            assert await custom_db.connection.fetchval("SELECT to_regnamespace($1)", target.schema_name) is None


def _arguments(fixture, **changes):
    return {
        "generation_id": fixture.source.generation_id,
        "provider_system": fixture.provider_system,
        "provider_id": fixture.provider_id,
        "control_schema": fixture.control_schema,
        **changes,
    }


async def _read(fixture, *, isolation="repeatable_read", **changes):
    async with fixture.connection.transaction(isolation=isolation, readonly=True):
        return await read_registry_source_sites(fixture.connection, **_arguments(fixture, **changes))


@pytest.mark.parametrize("catalog_db", [(2, "npi"), (2, "provider_directory")], indirect=True)
async def test_exact_provider_offices_and_wire_fields(catalog_db):
    fixture = catalog_db
    first = await _read(fixture, limit=1)
    second = await _read(fixture, limit=1, offset=1, isolation="serializable")
    complete = await _read(fixture)
    assert set(complete) == {
        "source_generation",
        "provider_system",
        "provider_id",
        "limit",
        "offset",
        "has_more",
        "sites",
    }
    assert complete["source_generation"] == fixture.source.generation_id
    assert len(complete["sites"]) == 2 and not complete["has_more"] and first["has_more"] and not second["has_more"]
    assert first["sites"] + second["sites"] == complete["sites"]
    for site in complete["sites"]:
        assert set(site) == {"fields", "display"} and validated_site_binding_fields(site["fields"]) == site["fields"]
        assert site["fields"]["provider_id"] == fixture.provider_id
        assert site["display"]["first_line"] == "123 Example Street"
        assert all(display_value is None or type(display_value) is str for display_value in site["display"].values())
    assert "canonical_network_ids" not in json.dumps(complete) and fixture.source.schema_name not in json.dumps(
        complete
    )
    assert "999 Other Street" not in json.dumps(complete)
    other = await _read(fixture, provider_id=fixture.other_id)
    assert len(other["sites"]) == 1 and other["sites"][0]["display"]["first_line"] == "999 Other Street"
    missing_id = "1000000020" if fixture.provider_system == "npi" else "unknown-directory-provider"
    assert (await _read(fixture, provider_id=missing_id))["sites"] == []
    assert (await _read(fixture, offset=1_000_000))["sites"] == []


async def test_native_full_row_hash_and_adoption(catalog_db):
    fixture = catalog_db
    result = await _read(fixture)
    fields = result["sites"][0]["fields"]
    native_hash = await fixture.connection.fetchval(
        f'''SELECT encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex')
          FROM "{fixture.source.schema_name}".entity_address_unified address WHERE location_key=$1''',
        fields["location_key"],
    )
    assert native_hash == fields["address_row_sha256"]
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        receipt = await resolve_retained_site_adoptions(
            fixture.connection,
            fixture.source,
            json.dumps([{key: value for key, value in fields.items() if key != "source_generation"}]).encode(),
            control_schema=fixture.control_schema,
        )
    assert receipt.records[0].address_row_sha256 == native_hash


@pytest.mark.parametrize(
    "changes",
    [
        {"generation_id": None},
        {"generation_id": True},
        {"generation_id": 0},
        {"generation_id": 9223372036854775808},
        {"provider_system": "manual"},
        {"provider_system": True},
        {"provider_id": 1000000004},
        {"provider_id": "1000000005"},
        {"provider_id": " 1000000004"},
        {"provider_id": "1000000004\n"},
        {"provider_system": "provider_directory", "provider_id": "x" * 129},
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"offset": -1},
        {"offset": 1_000_001},
        {"offset": False},
    ],
)
async def test_invalid_selector_has_no_database_calls(changes):
    with pytest.raises(RegistrySourceSiteCatalogError):
        await read_registry_source_sites(
            None, **{"generation_id": 1, "provider_system": "npi", "provider_id": "1000000004", **changes}
        )


async def test_transaction_contract_and_three_reads(catalog_db):
    fixture = catalog_db
    with pytest.raises(RegistrySourceSiteCatalogError):
        await read_registry_source_sites(fixture.connection, **_arguments(fixture))
    with pytest.raises(RegistrySourceSiteCatalogError):
        await _read(fixture, isolation="read_committed")
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            await read_registry_source_sites(fixture.connection, **_arguments(fixture, limit=1))
            await asyncio.sleep(0)
            assert len(queries) == 3 and all(query.query.lstrip().split()[0] in ("SELECT", "WITH") for query in queries)
    finally:
        fixture.connection.remove_query_logger(queries.append)


@pytest.mark.parametrize("catalog_db", [(100, "npi")], indirect=True)
async def test_maximum_page_uses_the_same_three_reads(catalog_db):
    fixture = catalog_db
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            result = await read_registry_source_sites(fixture.connection, **_arguments(fixture, limit=100))
            await asyncio.sleep(0)
            assert len(result["sites"]) == 100 and not result["has_more"] and len(queries) == 3
    finally:
        fixture.connection.remove_query_logger(queries.append)


@pytest.mark.parametrize(
    "damage",
    [
        "acl",
        "ineligible",
        "address_oid",
        "binding_oid",
        "manifest",
        "identity",
        "missing_address",
        "duplicate_key",
        "nil_site",
        "bad_key",
    ],
)
async def test_native_changes_fail_closed_before_pagination_and_rollback(catalog_db, damage):
    fixture = catalog_db
    connection = fixture.connection
    baseline = await _read(fixture)
    namespace = '"' + fixture.source.schema_name + '"'
    statements_by_damage = {
        "acl": f'GRANT UPDATE ON {namespace}.provider_location_binding TO "{fixture.roles["reader"]}"',
        "ineligible": f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false',
        "address_oid": f"ALTER TABLE {namespace}.entity_address_unified RENAME TO changed_address",
        "binding_oid": f"ALTER TABLE {namespace}.provider_location_binding RENAME TO changed_binding",
        "manifest": f'''UPDATE "{fixture.control_schema}".network_serving_manifest SET manifest_sha256=repeat('0',64)''',
        "identity": f"UPDATE {namespace}.provider_location_binding SET entity_id='unresolved-provider'",
        "missing_address": f"DELETE FROM {namespace}.entity_address_unified WHERE location_key=repeat('a',64)",
        "duplicate_key": f"""INSERT INTO {namespace}.provider_location_binding
          SELECT provider_system,provider_id,'{uuid4()}'::uuid,location_key,entity_type,entity_id
          FROM {namespace}.provider_location_binding WHERE location_key=repeat('a',64)""",
        "nil_site": f"""UPDATE {namespace}.provider_location_binding SET location_id='00000000-0000-0000-0000-000000000000'
          WHERE location_key=repeat('a',64)""",
        "bad_key": f"UPDATE {namespace}.provider_location_binding SET location_key=repeat('Z',64)",
    }
    transaction = connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await connection.execute(statements_by_damage[damage])
        with pytest.raises(RegistrySourceSiteCatalogUnavailable):
            await read_registry_source_sites(connection, **_arguments(fixture, limit=1, offset=1_000_000))
    finally:
        await transaction.rollback()
    assert await _read(fixture) == baseline


async def test_changed_native_row_rejects_the_prior_adoption_hash(catalog_db):
    fixture = catalog_db
    baseline = await _read(fixture)
    fields = baseline["sites"][0]["fields"]
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await fixture.connection.execute(
            f'''UPDATE "{fixture.source.schema_name}".entity_address_unified SET second_line='Suite changed'
              WHERE location_key=$1''',
            fields["location_key"],
        )
        current = await read_registry_source_sites(fixture.connection, **_arguments(fixture))
        assert current["sites"][0]["fields"]["address_row_sha256"] != fields["address_row_sha256"]
        with pytest.raises(RetainedSiteAdoptionError):
            await resolve_retained_site_adoptions(
                fixture.connection,
                fixture.source,
                json.dumps([{key: value for key, value in fields.items() if key != "source_generation"}]).encode(),
                control_schema=fixture.control_schema,
            )
    finally:
        await transaction.rollback()
    assert await _read(fixture) == baseline


async def test_response_byte_bound_rejects_whole_page(catalog_db):
    fixture = catalog_db
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await fixture.connection.execute(
            f'''UPDATE "{fixture.source.schema_name}".entity_address_unified SET first_line=repeat('x',1048576)
              WHERE entity_id=$1''',
            fixture.provider_id,
        )
        with pytest.raises(RegistrySourceSiteCatalogUnavailable):
            await read_registry_source_sites(fixture.connection, **_arguments(fixture, limit=1))
        assert await fixture.connection.fetchval("SELECT 1") == 1
    finally:
        await transaction.rollback()
    assert len((await _read(fixture))["sites"]) == 2


async def test_explicit_old_generation_does_not_follow_the_new_head(catalog_db):
    fixture = catalog_db
    baseline = await _read(fixture)
    source_table = f'"{fixture.source_schema}".entity_address_unified'
    target = await _raw_candidate(
        fixture, source_table, fixture.copy_targets, expected_head=fixture.source.generation_id
    )
    await prepare_and_publish_network_candidate(
        fixture.connection, target, fixture.base, **_roles(fixture), control_schema=fixture.control_schema
    )
    current = await fixture.connection.fetchval(
        f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
    )
    assert current != fixture.source.generation_id
    assert await _read(fixture) == baseline
    newer = await _read(fixture, generation_id=current)
    assert newer["source_generation"] == current
    assert newer["sites"][0]["fields"]["address_row_sha256"] != baseline["sites"][0]["fields"]["address_row_sha256"]


async def test_ordinary_reader_has_no_owner_or_write_privileges(catalog_db):
    fixture = catalog_db
    connection = fixture.connection
    role = '"' + fixture.roles["reader"] + '"'
    control = '"' + fixture.control_schema + '"'
    await connection.execute(f"GRANT USAGE ON SCHEMA {control} TO {role}")
    await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {control} TO {role}")
    baseline = await _read(fixture)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await connection.execute(f"SET LOCAL ROLE {role}")
        assert await read_registry_source_sites(connection, **_arguments(fixture)) == baseline
        assert not await connection.fetchval(
            "SELECT has_table_privilege(current_user,$1,'UPDATE') OR has_schema_privilege(current_user,$2,'CREATE')",
            '"' + fixture.source.schema_name + '".entity_address_unified',
            fixture.source.schema_name,
        )


@pytest.mark.parametrize(
    "field,maximum",
    [
        ("entity_name", 512),
        ("first_line", 512),
        ("second_line", 512),
        ("city_name", 128),
        ("state_name", 64),
        ("postal_code", 32),
        ("country_code", 64),
    ],
)
async def test_display_is_bounded_or_the_whole_page_fails(catalog_db, field, maximum):
    fixture = catalog_db
    namespace = '"' + fixture.source.schema_name + '"'
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        if field == "entity_name":
            await fixture.connection.execute(
                f"ALTER TABLE {namespace}.entity_address_unified ALTER COLUMN entity_name TYPE text"
            )
        await fixture.connection.execute(
            f'UPDATE {namespace}.entity_address_unified SET "{field}"=$1 WHERE entity_id=$2',
            "x" * maximum,
            fixture.provider_id,
        )
        result = await read_registry_source_sites(fixture.connection, **_arguments(fixture))
        assert all(len(site["display"][field]) == maximum for site in result["sites"])
        await fixture.connection.execute(
            f'UPDATE {namespace}.entity_address_unified SET "{field}"=$1 WHERE entity_id=$2',
            "x" * (maximum + 1),
            fixture.provider_id,
        )
        with pytest.raises(RegistrySourceSiteCatalogUnavailable):
            await read_registry_source_sites(fixture.connection, **_arguments(fixture))
    finally:
        await transaction.rollback()


async def test_caller_snapshot_remains_pinned_after_eligibility_changes(catalog_db):
    fixture = catalog_db
    postgres_dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://", 1)
    other_connection = await asyncpg.connect(postgres_dsn)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            baseline = await read_registry_source_sites(fixture.connection, **_arguments(fixture))
            await other_connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
                fixture.source.generation_id,
            )
            assert await read_registry_source_sites(fixture.connection, **_arguments(fixture)) == baseline
        with pytest.raises(RegistrySourceSiteCatalogUnavailable):
            await _read(fixture)
    finally:
        await other_connection.close()
