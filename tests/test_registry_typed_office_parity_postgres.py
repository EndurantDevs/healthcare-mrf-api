# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded typed parity keeps legacy semantics under a dense office alias bucket."""

import json
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process.network_custom_address_source import _APPROVED_CTE
from process.network_initial_source_office_bindings import copy_source_office_binding_rows
from process.registry_initial_source_composition import RegistryInitialSourceError
from process.registry_source_office_custody import _binding_parity
from process.registry_typed_office_parity import create_source_pair_index, typed_binding_parity
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


async def _prepare_custom_override(connection, control, actual, expected):
    site = await connection.fetchval(f"SELECT min(location_id::text)::uuid FROM {expected}")
    source_records = (
        ("location", str(site), {"archived": False}),
        (
            "membership",
            str(uuid4()),
            {
                "archived": False,
                "memberships_json": [
                    {
                        "network_id": 1,
                        "provider_system": "npi",
                        "provider_id": "1234567893",
                        "location_id": str(site),
                    }
                ],
            },
        ),
    )
    for kind, key, document in source_records:
        await connection.execute(
            f'INSERT INTO "{control}".registry_approved_record VALUES(2,$1,$2,1,1,$3::jsonb)',
            kind,
            key,
            json.dumps(document),
        )
    custom_key = await connection.fetchval(
        _APPROVED_CTE.format(control='"' + control + '"') + "SELECT location_key FROM custom", 2
    )
    await connection.execute(f"UPDATE {actual} SET location_key=$1 WHERE location_id=$2", custom_key, site)
    return site


async def _prepare_dense_office_fixture(connection, control, name, reader, relations):
    namespace, actual, source_relation, expected, carried = relations
    async with connection.transaction():
        for table in ("provider_location_binding", "expected", "carried"):
            await copy_source_office_binding_rows(connection, name, table, ())
        await connection.execute(
            f"CREATE INDEX typed_office_location_idx ON {actual}"
            "(location_key,provider_system,provider_id,entity_type,entity_id)"
        )
        await connection.execute(
            f"CREATE TABLE {source_relation}(network_id integer NOT NULL,provider_system text NOT NULL,"
            "provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL)"
        )
        await create_source_pair_index(connection, source_relation)
        await connection.execute(
            f"INSERT INTO {actual} SELECT 'npi','1234567893',md5(i::text)::uuid,repeat('a',64),"
            "'npi','1234567893' FROM generate_series(1,20000) i"
        )
        # All 20,000 sites share one canonical office; duplicate evidence is
        # legitimate. The preserved range also overlaps one identical row.
        await connection.execute(
            f"INSERT INTO {source_relation} SELECT 1,provider_system,provider_id,location_id,'first' FROM {actual}"
        )
        await connection.execute(
            f"INSERT INTO {source_relation} SELECT 2,provider_system,provider_id,location_id,'second' FROM {actual}"
        )
        await connection.execute(f"INSERT INTO {expected} SELECT * FROM {actual} ORDER BY location_id LIMIT 19001")
        await connection.execute(f"INSERT INTO {carried} SELECT * FROM {actual} ORDER BY location_id OFFSET 19000")
        site = await _prepare_custom_override(connection, control, actual, expected)
        await connection.execute(f'GRANT USAGE ON SCHEMA {namespace},"{control}" TO "{reader}"')
        await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO "{reader}"')
        await connection.execute(f'GRANT SELECT ON "{control}".registry_approved_record TO "{reader}"')
        for relation in (actual, source_relation, expected, carried):
            await connection.execute(f"ANALYZE {relation}")
    return site


async def _check_dense_office_parity(
    connection, control, name, reader, relations, timings_by_case, label, invalid=False
):
    _namespace, _actual, _source_relation, expected, carried = relations
    arguments = (
        connection,
        SimpleNamespace(schema_name=name),
        expected,
        {"approved_custom_revision": 2},
        control,
    )
    preservation = SimpleNamespace(binding_query=f"SELECT DISTINCT * FROM {carried}")
    is_readonly = not connection.is_in_transaction()
    async with connection.transaction(isolation="repeatable_read", readonly=is_readonly):
        await connection.execute("SET LOCAL work_mem='4MB'")
        await connection.execute("SET LOCAL statement_timeout='120s'")
        await connection.execute("SET LOCAL temp_file_limit='256MB'")
        await connection.execute(f'SET LOCAL ROLE "{reader}"')
        assert not await connection.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
        assert await connection.fetchval("SHOW transaction_read_only") == ("on" if is_readonly else "off")
        assert await connection.fetchval("SHOW work_mem") == "4MB"
        assert await connection.fetchval("SHOW temp_file_limit") == "256MB"
        for function in (_binding_parity, typed_binding_parity):
            started = monotonic()
            if invalid:
                with pytest.raises(RegistryInitialSourceError, match="bindings_differ"):
                    await function(*arguments, preservation=preservation)
            else:
                await function(*arguments, preservation=preservation)
            timings_by_case[label + "/" + function.__name__] = monotonic() - started


async def _assert_dense_office_corruptions(connection, relations, site, check):
    _namespace, actual, source_relation, expected, carried = relations
    damage_cases = (
        (f"DELETE FROM {actual} WHERE location_id=$1", (site,)),
        (f"UPDATE {actual} SET entity_id='wrong' WHERE location_id=$1", (site,)),
        (f"UPDATE {actual} SET location_key=repeat('f',64) WHERE location_id=$1", (site,)),
        (f"INSERT INTO {source_relation} VALUES(1,'npi','absent',md5('absent')::uuid,'extra')", ()),
        (f"INSERT INTO {expected} VALUES('npi','absent',md5('absent')::uuid,repeat('b',64),'npi','absent')", ()),
        (f"INSERT INTO {actual} VALUES('npi','other',md5('other')::uuid,repeat('a',64),'npi','other')", ()),
        (
            f"INSERT INTO {actual} SELECT 'manual','manual',md5(i::text)::uuid,repeat('b',64),'manual','manual' FROM generate_series(1,2) i",
            (),
        ),
        (
            f"INSERT INTO {carried} SELECT provider_system,provider_id,location_id,repeat('c',64),entity_type,entity_id FROM {expected} WHERE location_id=$1",
            (site,),
        ),
    )
    # Each corruption is rolled back before the next comparison, preserving
    # the identical baseline and avoiding any relaxed candidate write scope.
    for ordinal, (statement, arguments) in enumerate(damage_cases):
        transaction = connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await connection.execute(statement, *arguments)
            await check(f"damage-{ordinal}", invalid=True)
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_typed_binding_parity_dense_aliases_custom_preservation_and_corruption(serving_schema, record_property):
    """Keep custom overrides, alias multiplicity and corruption refusal equal to legacy parity."""
    connection, control, _ = serving_schema
    name = "typed_office_parity_" + uuid4().hex
    namespace = '"' + name + '"'
    reader = "typed_office_reader_" + uuid4().hex
    actual = namespace + ".provider_location_binding"
    source_relation = namespace + ".network_membership_source"
    expected = namespace + ".expected"
    carried = namespace + ".carried"
    timings_by_case = {}
    await connection.execute(f'CREATE ROLE "{reader}" NOLOGIN NOSUPERUSER')
    await connection.execute(f"CREATE SCHEMA {namespace}")
    try:
        relations = namespace, actual, source_relation, expected, carried
        site = await _prepare_dense_office_fixture(connection, control, name, reader, relations)

        async def check(label, invalid=False):
            await _check_dense_office_parity(
                connection, control, name, reader, relations, timings_by_case, label, invalid
            )

        await check("valid")
        await _assert_dense_office_corruptions(connection, relations, site, check)
        await check("restored")
        record_property("typed_office_parity_seconds", json.dumps(timings_by_case, sort_keys=True))
        record_property(
            "typed_office_parity_shape",
            "20000 sites; one shared office; 40000 source rows; custom override; preserved overlap",
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {namespace} CASCADE")
        await connection.execute(f'REVOKE USAGE ON SCHEMA "{control}" FROM "{reader}"')
        await connection.execute(f'REVOKE SELECT ON "{control}".registry_approved_record FROM "{reader}"')
        await connection.execute(f'DROP ROLE "{reader}"')
        assert await connection.fetchval("SELECT to_regnamespace($1)", name) is None
