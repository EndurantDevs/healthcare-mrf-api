# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source publication observes finite registry references without mutation rights."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_compact_retention as retention
from process.provider_directory_projection_stage import _copy_driver
from tests.fhir_catalog_source_control_read_support import _grant_source_retention_control_reads
from tests.provider_directory_source_postgres_support import native_source_component
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

_CONTROLS = ("network_membership_candidate", "network_serving_manifest", "network_candidate_custody")


@asynccontextmanager
async def _source_control_case(serving, monkeypatch):
    connection, control_schema, engine = serving
    schema = "source_control_test_" + uuid4().hex
    is_created = False
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        is_created = True
        async with native_source_component(engine, schema, monkeypatch) as component:
            yield SimpleNamespace(
                component=component,
                fixture=SimpleNamespace(connection=connection, control_schema=control_schema),
                source_schema=schema,
            )
    finally:
        if is_created:
            await connection.execute(f'DROP SCHEMA "{schema}" RESTRICT')


async def _guard(case):
    component = case.component
    async with component.factory.transaction(component.database, isolation="read committed") as session:
        connection = await _copy_driver(session)
        assert await connection.fetchval("SELECT session_user::text") == component.publisher
        assert await connection.fetchval("SELECT current_user::text") == component.owner
        await retention.require_named_address_artifacts_unreferenced(
            connection,
            [(case.source_schema, "entity_address_unified")],
            control_schema=case.fixture.control_schema,
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_locks locked JOIN pg_class c ON c.oid=locked.relation "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE locked.pid=pg_backend_pid() "
                "AND locked.mode='AccessShareLock' AND locked.granted AND n.nspname=$1 "
                "AND c.relname=ANY($2::text[])",
                case.fixture.control_schema,
                list(_CONTROLS[:2]),
            )
            == 2
        )


async def _assert_control_mutations_refused(case, tables):
    component = case.component
    async with component.factory.transaction(component.database, isolation="read committed") as session:
        connection = await _copy_driver(session)
        for table in tables:
            relation = f'"{case.fixture.control_schema}"."{table}"'
            for statement in (
                f"INSERT INTO {relation}(candidate_id) SELECT candidate_id FROM {relation} WHERE false",
                f"UPDATE {relation} SET candidate_id=candidate_id WHERE false",
                f"DELETE FROM {relation} WHERE false",
                f"TRUNCATE {relation}",
            ):
                await _assert_mutation_refused(connection, statement)
        assert not await connection.fetchval(
            "SELECT has_schema_privilege(current_user,$1,'CREATE')", case.fixture.control_schema
        )
        assert not await connection.fetchval(
            "SELECT has_table_privilege(current_user,$1,'SELECT')",
            f'"{case.fixture.control_schema}".registry_revision_control',
        )


async def _assert_mutation_refused(connection, statement):
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(statement)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_ledger", [False, True])
async def test_source_control_reads_allow_guard_without_mutation(serving_schema, monkeypatch, has_ledger):
    async with _source_control_case(serving_schema, monkeypatch) as case:
        tables = _CONTROLS if has_ledger else _CONTROLS[:2]
        if not has_ledger:
            await case.fixture.connection.execute(
                f'DROP TABLE "{case.fixture.control_schema}".network_candidate_custody RESTRICT'
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await _guard(case)
        await _grant_source_retention_control_reads(case)
        await _guard(case)
        await _assert_control_mutations_refused(case, tables)
        for table in tables:
            assert (
                await case.fixture.connection.fetchval(
                    f'SELECT count(*) FROM "{case.fixture.control_schema}"."{table}"'
                )
                == 0
            )
        await _assert_reference_refusal(case)


async def _assert_reference_refusal(case):
    connection, schema = case.fixture.connection, case.fixture.control_schema
    candidate_id = uuid4()
    await _candidate(connection, schema, candidate_id)
    report_by_field = {
        "valid": True,
        "validation_revision": 1,
        "source_generations": {"unified_address": "source-edition"},
        "address_source": {
            "schema_name": case.source_schema,
            "table_name": "entity_address_unified",
            "generation_id": "source-edition",
        },
    }
    await connection.execute(
        f"UPDATE \"{schema}\".network_membership_candidate SET state='validated',validation_json=$2::jsonb "
        "WHERE candidate_id=$1",
        candidate_id,
        json.dumps(report_by_field),
    )
    query = f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1'
    before = await connection.fetchrow(query, candidate_id)
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await _guard(case)
    assert await connection.fetchrow(query, candidate_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    [
        None,
        {},
        {"table_name": "entity_address_unified"},
        {"schema_name": None},
        {"schema_name": 7},
        {"schema_name": []},
    ],
)
async def test_named_reference_scan_refuses_missing_or_nonstring_source_schema(serving_schema, source):
    connection, schema, _ = serving_schema
    candidate_id = uuid4()
    await _candidate(connection, schema, candidate_id)
    await connection.execute(
        f"UPDATE \"{schema}\".network_membership_candidate SET state='ready',index_ready=true,validation_json=$2::jsonb "
        "WHERE candidate_id=$1",
        candidate_id,
        json.dumps({"address_source": source}),
    )
    query = f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1'
    before = await connection.fetchrow(query, candidate_id)
    with pytest.raises(retention.NetworkCompactRetentionError, match="address_reference_invalid"):
        async with connection.transaction():
            await retention.require_named_address_artifacts_unreferenced(
                connection, [("example", "entity_address_unified")], control_schema=schema
            )
    assert await connection.fetchrow(query, candidate_id) == before
