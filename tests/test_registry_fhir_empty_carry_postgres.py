# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An empty retained selection avoids predecessor scans without weakening parity."""

import json
import os
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import registry_typed_office_parity as parity
from process.network_initial_source_office_bindings import copy_source_office_binding_rows
from process.registry_fhir_serving_preservation import _carry_queries
from process.registry_initial_source_composition import RegistryInitialSourceError
from process.registry_source_office_custody import _binding_parity


@pytest.fixture
async def carry_case():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    prior_schema = "carry_parity_" + uuid4().hex
    candidate_schema = "carry_parity_" + uuid4().hex
    try:
        for schema in (prior_schema, candidate_schema):
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            async with connection.transaction():
                await copy_source_office_binding_rows(connection, schema, "provider_location_binding", ())
            await connection.execute(
                f'CREATE TABLE "{schema}".network_membership_source(network_id integer,provider_system text,'
                "provider_id text,location_id uuid,evidence_id text)"
            )
        await parity.create_source_pair_index(connection, f'"{candidate_schema}".network_membership_source')
        await connection.execute(
            f'CREATE TABLE "{candidate_schema}".registry_approved_record('
            "approved_revision bigint,record_kind text,record_key text,record_json jsonb)"
        )
        await connection.execute("CREATE TEMP TABLE missing(network_id integer PRIMARY KEY)")
        async with connection.transaction():
            await copy_source_office_binding_rows(connection, "pg_temp", "expected", ())
        for schema, providers in ((prior_schema, ("20", "40", "60", "zz-carried")), ("pg_temp", ("01", "02", "03"))):
            table = "expected" if schema == "pg_temp" else "provider_location_binding"
            async with connection.transaction():
                await copy_source_office_binding_rows(
                    connection,
                    schema,
                    table,
                    [
                        ("npi", provider, UUID(int=1), provider.ljust(64, "0"), "npi", provider)
                        for provider in providers
                    ],
                )
        await connection.execute(
            f'INSERT INTO "{prior_schema}".network_membership_source '
            "SELECT CASE provider_id WHEN '40' THEN 2 WHEN 'zz-carried' THEN 3 ELSE 1 END,"
            f'provider_system,provider_id,location_id,e::text FROM "{prior_schema}".provider_location_binding '
            "CROSS JOIN generate_series(1,3) e"
        )
        query = _carry_queries(
            SimpleNamespace(schema_name=prior_schema), "pg_temp.missing", f'"{prior_schema}".network_membership_source'
        )[1]
        yield connection, prior_schema, candidate_schema, query
    finally:
        for schema in (candidate_schema, prior_schema):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def _check_parity(connection, candidate_schema, query, *, invalid=False):
    async with connection.transaction(isolation="repeatable_read", readonly=not connection.is_in_transaction()):
        for function in (_binding_parity, parity.typed_binding_parity):
            arguments = (
                connection,
                SimpleNamespace(schema_name=candidate_schema),
                "pg_temp.expected",
                {"approved_custom_revision": 1},
                candidate_schema,
            )
            if invalid:
                with pytest.raises(RegistryInitialSourceError, match="bindings_differ"):
                    await function(*arguments, preservation=SimpleNamespace(binding_query=query))
            else:
                await function(*arguments, preservation=SimpleNamespace(binding_query=query))


@pytest.mark.parametrize("networks", [(), (1,), (1, 2)], ids=["empty", "sparse", "complete"])
async def test_carry_selection_and_both_parity_tails(carry_case, monkeypatch, networks):
    connection, prior_schema, candidate_schema, query = carry_case
    monkeypatch.setattr(parity, "_PAGE_SIZE", 2)
    await connection.executemany("INSERT INTO pg_temp.missing VALUES($1)", [(network_id,) for network_id in networks])
    legacy = f'''SELECT DISTINCT b.* FROM "{prior_schema}".provider_location_binding b
      WHERE EXISTS(SELECT FROM "{prior_schema}".network_membership_source m JOIN pg_temp.missing ids USING(network_id)
        WHERE (m.provider_system,m.provider_id,m.location_id)=(b.provider_system,b.provider_id,b.location_id))'''
    assert not await connection.fetchval(
        f"SELECT EXISTS((({query}) EXCEPT ALL ({legacy})) UNION ALL (({legacy}) EXCEPT ALL ({query})))"
    )
    await connection.execute(
        f'INSERT INTO "{candidate_schema}".provider_location_binding SELECT * FROM pg_temp.expected UNION {query}'
    )
    await connection.execute(
        f'INSERT INTO "{candidate_schema}".network_membership_source SELECT 1,provider_system,provider_id,location_id,e::text '
        f'FROM "{candidate_schema}".provider_location_binding CROSS JOIN generate_series(1,2) e'
    )
    await _check_parity(connection, candidate_schema, query)
    for damage in (
        "INSERT INTO pg_temp.missing VALUES(3)",
        f'''INSERT INTO "{candidate_schema}".network_membership_source VALUES(1,'npi','zz-source','{UUID(int=2)}','extra')''',
        f"""INSERT INTO pg_temp.expected VALUES('npi','zz-expected','{UUID(int=3)}',repeat('x',64),'npi','zz-expected')""",
        f'''UPDATE "{candidate_schema}".provider_location_binding SET entity_id='wrong' WHERE provider_id='03' ''',
    ):
        transaction = connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await connection.execute(damage)
            await _check_parity(connection, candidate_schema, query, invalid=True)
        finally:
            await transaction.rollback()
    await _check_parity(connection, candidate_schema, query)


@pytest.mark.parametrize("after", [None, ("npi", "01", UUID(int=1))], ids=["first", "later"])
async def test_empty_carry_upper_bound_never_scans_predecessor(carry_case, after):
    connection, _prior, _target, query = carry_case
    statements = []

    async def capture(statement, *arguments):
        statements.append((statement, arguments))

    await parity._upper_bound(SimpleNamespace(fetchrow=capture), [query], after)
    statement, arguments = statements[0]
    async with connection.transaction(readonly=True):
        document = json.loads(await connection.fetchval("EXPLAIN (ANALYZE, FORMAT JSON) " + statement, *arguments))
    pending_plans = [document[0]["Plan"]]
    predecessor_scans = []
    while pending_plans:
        node = pending_plans.pop()
        pending_plans.extend(node.get("Plans", ()))
        if node.get("Relation Name") in {"provider_location_binding", "network_membership_source"}:
            predecessor_scans.append(node)
    assert predecessor_scans
    assert all(node["Actual Loops"] == 0 for node in predecessor_scans)
